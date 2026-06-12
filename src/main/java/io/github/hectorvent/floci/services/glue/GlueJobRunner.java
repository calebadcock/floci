package io.github.hectorvent.floci.services.glue;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.WaitResponse;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.services.glue.model.Job;
import io.github.hectorvent.floci.services.glue.model.JobCommand;
import io.github.hectorvent.floci.services.glue.model.JobRun;
import io.github.hectorvent.floci.services.s3.S3Service;
import io.github.hectorvent.floci.services.s3.model.S3Object;
import io.quarkus.runtime.ShutdownEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.jboss.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@ApplicationScoped
public class GlueJobRunner {

    private static final Logger LOG = Logger.getLogger(GlueJobRunner.class);
    private static final String SCRIPT_PATH = "/tmp/floci-glue/script.py";
    private static final String LOG_GROUP = "/aws-glue/jobs/output";

    private final DockerClient dockerClient;
    private final ContainerBuilder containerBuilder;
    private final ContainerLifecycleManager lifecycleManager;
    private final ContainerLogStreamer logStreamer;
    private final S3Service s3Service;
    private final EmulatorConfig config;
    private final ContainerDetector containerDetector;
    private final ConcurrentHashMap<String, String> runningContainers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ActiveRun> activeRuns = new ConcurrentHashMap<>();

    @Inject
    public GlueJobRunner(DockerClient dockerClient,
                         ContainerBuilder containerBuilder,
                         ContainerLifecycleManager lifecycleManager,
                         ContainerLogStreamer logStreamer,
                         S3Service s3Service,
                         EmulatorConfig config,
                         ContainerDetector containerDetector) {
        this.dockerClient = dockerClient;
        this.containerBuilder = containerBuilder;
        this.lifecycleManager = lifecycleManager;
        this.logStreamer = logStreamer;
        this.s3Service = s3Service;
        this.config = config;
        this.containerDetector = containerDetector;
    }

    public void startJobRun(String region, Job job, JobRun run, Consumer<JobRun> onUpdate) {
        Thread.ofVirtual().start(() -> runJob(region, job, run, onUpdate));
    }

    public void stopJobRun(String runId) {
        String containerId = runningContainers.get(runId);
        if (containerId == null) {
            return;
        }
        try {
            dockerClient.stopContainerCmd(containerId).withTimeout(5).exec();
        } catch (Exception e) {
            LOG.debugv("Error stopping Glue job container {0}: {1}", containerId, e.getMessage());
        }
    }

    void onStop(@Observes ShutdownEvent ignored) {
        stopAll();
    }

    public void stopAll() {
        if (!runningContainers.isEmpty()) {
            LOG.infov("Stopping {0} Glue job container(s) on shutdown", runningContainers.size());
        }
        runningContainers.forEach((runId, containerId) -> lifecycleManager.stopAndRemove(containerId, null));
        runningContainers.clear();
        activeRuns.forEach((runId, active) -> {
            if (GlueService.isTerminalJobRunState(active.run().getJobRunState())) {
                return;
            }
            try {
                transition(active.run(), "STOPPED", null,
                        "Job run stopped because the emulator is shutting down", active.onUpdate());
            } catch (Exception e) {
                LOG.warnv("Error marking Glue job run {0} as stopped: {1}", runId, e.getMessage());
            }
        });
        activeRuns.clear();
    }

    private void runJob(String region, Job job, JobRun run, Consumer<JobRun> onUpdate) {
        Instant started = Instant.now();
        activeRuns.put(run.getId(), new ActiveRun(run, onUpdate));
        try {
            run.setLogGroupName(LOG_GROUP);
            run.setLogStreamName(logStreamName(job, run));
            transition(run, "RUNNING", null, "Preparing Glue job run", onUpdate);

            if (config.services().glue().mock()) {
                transition(run, "SUCCEEDED", null, "Completed by mock Glue runner", onUpdate);
                return;
            }

            byte[] script = loadScript(job.getCommand());
            String command = containerCommand(job, run);
            int attempts = maxAttempts(job);
            for (int attempt = 0; attempt < attempts; attempt++) {
                if ("STOPPED".equals(run.getJobRunState())) {
                    return;
                }
                run.setAttempt(attempt);
                transition(run, "RUNNING", null, attemptDetail(attempt, attempts), onUpdate);

                AttemptResult result = runContainerAttempt(region, job, run, script, command);
                if ("STOPPED".equals(run.getJobRunState())) {
                    return;
                }
                if (result.success()) {
                    transition(run, "SUCCEEDED", null, "Glue job completed successfully", onUpdate);
                    return;
                }
                if (result.timedOut()) {
                    stopJobRun(run.getId());
                    transition(run, "TIMEOUT",
                            "Job timed out after " + timeoutMinutes(run) + " minutes",
                            result.message(), onUpdate);
                    return;
                }
                if (attempt + 1 < attempts) {
                    transition(run, "RUNNING", null,
                            result.message() + "; retrying attempt " + (attempt + 2) + " of " + attempts,
                            onUpdate);
                    continue;
                }
                transition(run, "FAILED", result.message(), result.message(), onUpdate);
                return;
            }
        } catch (Exception e) {
            LOG.warnv("Glue job run {0} failed: {1}", run.getId(), e.getMessage());
            transition(run, "FAILED", e.getMessage(), e.getMessage(), onUpdate);
        } finally {
            runningContainers.remove(run.getId());
            activeRuns.remove(run.getId());
            run.setExecutionTime((int) Duration.between(started, Instant.now()).toSeconds());
            onUpdate.accept(run);
        }
    }

    private void transition(JobRun run, String state, String errorMessage,
                            String stateDetail, Consumer<JobRun> onUpdate) {
        Instant now = Instant.now();
        run.setJobRunState(state);
        run.setLastModifiedOn(now);
        run.setStateDetail(stateDetail);
        if (GlueService.isTerminalJobRunState(state)) {
            run.setCompletedOn(now);
        }
        if (errorMessage != null && !errorMessage.isBlank()) {
            run.setErrorMessage(errorMessage);
        }
        onUpdate.accept(run);
    }

    private AttemptResult runContainerAttempt(String region, Job job, JobRun run,
                                              byte[] script, String command) {
        String containerId = null;
        Closeable logHandle = null;
        try {
            ContainerSpec spec = containerBuilder.newContainer(imageFor(job))
                    .withEntrypoint(List.of("sh", "-lc"))
                    .withCmd(List.of(command))
                    .withEnv(env(region, job, run))
                    .withDockerNetwork(config.services().glue().dockerNetwork())
                    .withEmbeddedDns()
                    .withHostDockerInternalOnLinux()
                    .withLogRotation()
                    .build();

            containerId = lifecycleManager.create(spec);
            runningContainers.put(run.getId(), containerId);
            copyScriptToContainer(containerId, script);
            lifecycleManager.startCreated(containerId, spec);
            logHandle = attachLogs(containerId, region, job, run);

            Integer exitCode = waitForContainer(containerId, timeoutMinutes(run));
            if (exitCode != null && exitCode == 0) {
                return AttemptResult.succeeded();
            }
            if (exitCode == null) {
                return AttemptResult.timedOut("Container did not exit before timeout");
            }
            return AttemptResult.failed("Container exited with code " + exitCode);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return AttemptResult.failed("Interrupted while waiting for Glue job container");
        } catch (Exception e) {
            return AttemptResult.failed(e.getMessage());
        } finally {
            if (containerId != null) {
                runningContainers.remove(run.getId(), containerId);
                lifecycleManager.stopAndRemove(containerId, logHandle);
            }
        }
    }

    private Closeable attachLogs(String containerId, String region, Job job, JobRun run) {
        if (logStreamer == null) {
            return null;
        }
        return logStreamer.attach(containerId, run.getLogGroupName(), run.getLogStreamName(),
                region, "glue:" + job.getName() + ":" + run.getId());
    }

    private byte[] loadScript(JobCommand command) throws IOException {
        String location = command != null ? command.getScriptLocation() : null;
        if (location == null || location.isBlank()) {
            throw new AwsException("InvalidInputException", "Command.ScriptLocation is required", 400);
        }
        if (location.startsWith("s3://")) {
            int slash = location.indexOf('/', "s3://".length());
            if (slash < 0 || slash == location.length() - 1) {
                throw new AwsException("InvalidInputException", "Invalid S3 script location: " + location, 400);
            }
            String bucket = location.substring("s3://".length(), slash);
            String key = location.substring(slash + 1);
            S3Object obj = s3Service.getObject(bucket, key);
            if (obj == null || obj.getData() == null) {
                throw new AwsException("EntityNotFoundException", "Script not found: " + location, 400);
            }
            return obj.getData();
        }
        if (location.startsWith("file://")) {
            return Files.readAllBytes(Path.of(location.substring("file://".length())));
        }
        return Files.readAllBytes(Path.of(location));
    }

    private void copyScriptToContainer(String containerId, byte[] script) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(out)) {
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
            TarArchiveEntry dir = new TarArchiveEntry("floci-glue/");
            tar.putArchiveEntry(dir);
            tar.closeArchiveEntry();
            TarArchiveEntry file = new TarArchiveEntry("floci-glue/script.py");
            file.setMode(0755);
            file.setSize(script.length);
            tar.putArchiveEntry(file);
            tar.write(script);
            tar.closeArchiveEntry();
        }
        dockerClient.copyArchiveToContainerCmd(containerId)
                .withRemotePath("/tmp")
                .withTarInputStream(new ByteArrayInputStream(out.toByteArray()))
                .exec();
    }

    String imageFor(Job job) {
        String glueVersion = job.getGlueVersion();
        if (glueVersion == null || glueVersion.isBlank()) {
            return config.services().glue().defaultJobImage();
        }
        String normalized = glueVersion.strip();
        if (normalized.startsWith("4")) {
            return config.services().glue().glue4JobImage();
        }
        if (normalized.startsWith("5")) {
            return config.services().glue().glue5JobImage();
        }
        return config.services().glue().defaultJobImage();
    }

    String containerCommand(Job job, JobRun run) {
        String commandName = job.getCommand() != null ? job.getCommand().getName() : null;
        boolean python = commandName != null && commandName.toLowerCase().contains("python");
        String runner = python ? pythonRunnerDiscovery() : sparkRunnerDiscovery();
        String prelude = !python && icebergRequested(run) ? icebergJarsDiscovery() : "";
        String sparkArguments = python ? "" : sparkSubmitArguments(run);
        return "set -e; " + runner + "; " + prelude + "exec \"$FLOCI_GLUE_RUNNER\" "
                + sparkArguments + shellQuote(SCRIPT_PATH) + argumentString(run.getArguments());
    }

    private String sparkRunnerDiscovery() {
        return "if command -v spark-submit >/dev/null 2>&1; then "
                + "FLOCI_GLUE_RUNNER=$(command -v spark-submit); "
                + "elif command -v gluesparksubmit >/dev/null 2>&1; then "
                + "FLOCI_GLUE_RUNNER=$(command -v gluesparksubmit); "
                + "elif [ -x /home/hadoop/aws-glue-libs/bin/gluesparksubmit ]; then "
                + "FLOCI_GLUE_RUNNER=/home/hadoop/aws-glue-libs/bin/gluesparksubmit; "
                + "elif [ -x /home/glue_user/spark/bin/spark-submit ]; then "
                + "FLOCI_GLUE_RUNNER=/home/glue_user/spark/bin/spark-submit; "
                + "elif [ -x /home/glue_user/aws-glue-libs/bin/gluesparksubmit ]; then "
                + "FLOCI_GLUE_RUNNER=/home/glue_user/aws-glue-libs/bin/gluesparksubmit; "
                + "else echo 'No Glue Spark runner found in image' >&2; exit 127; fi";
    }

    private String pythonRunnerDiscovery() {
        return "if command -v python3 >/dev/null 2>&1; then "
                + "FLOCI_GLUE_RUNNER=$(command -v python3); "
                + "elif command -v python >/dev/null 2>&1; then "
                + "FLOCI_GLUE_RUNNER=$(command -v python); "
                + "else echo 'No Python runner found in image' >&2; exit 127; fi";
    }

    private String sparkSubmitArguments(JobRun run) {
        StringBuilder sb = new StringBuilder();
        appendSparkConf(sb, "spark.hadoop.fs.s3a.endpoint", resolveEndpointUrl());
        appendSparkConf(sb, "spark.hadoop.fs.s3a.path.style.access", "true");
        appendSparkConf(sb, "spark.hadoop.fs.s3a.connection.ssl.enabled", "false");
        appendSparkConf(sb, "spark.hadoop.fs.s3a.access.key", "test");
        appendSparkConf(sb, "spark.hadoop.fs.s3a.secret.key", "test");
        if (icebergRequested(run)) {
            sb.append("${FLOCI_GLUE_ICEBERG_JARS:+--jars \"$FLOCI_GLUE_ICEBERG_JARS\"} ");
            appendSparkConf(sb, "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        }
        return sb.toString();
    }

    private static void appendSparkConf(StringBuilder sb, String key, String value) {
        sb.append("--conf ").append(shellQuote(key + "=" + value)).append(' ');
    }

    private static String icebergJarsDiscovery() {
        return "FLOCI_GLUE_ICEBERG_JARS=$(find /usr/share/aws/datalake-formats/iceberg -name '*.jar' "
                + "2>/dev/null | paste -sd, -); ";
    }

    private static boolean icebergRequested(JobRun run) {
        Map<String, String> arguments = run.getArguments();
        String formats = arguments != null ? arguments.get("--datalake-formats") : null;
        return formats != null && formats.toLowerCase().contains("iceberg");
    }

    private String argumentString(Map<String, String> arguments) {
        if (arguments == null || arguments.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        arguments.forEach((key, value) -> {
            sb.append(' ').append(shellQuote(key));
            if (value != null && !value.isBlank()) {
                sb.append(' ').append(shellQuote(value));
            }
        });
        return sb.toString();
    }

    private List<String> env(String region, Job job, JobRun run) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("AWS_DEFAULT_REGION", region);
        env.put("AWS_REGION", region);
        env.put("AWS_ACCESS_KEY_ID", "test");
        env.put("AWS_SECRET_ACCESS_KEY", "test");
        env.put("AWS_ENDPOINT_URL", resolveEndpointUrl());
        env.put("GLUE_JOB_NAME", job.getName());
        env.put("GLUE_JOB_RUN_ID", run.getId());
        env.put("AWS_GLUE_LOG_GROUP_NAME", run.getLogGroupName());
        env.put("AWS_GLUE_LOG_STREAM_NAME", run.getLogStreamName());
        List<String> result = new ArrayList<>();
        env.forEach((key, value) -> result.add(key + "=" + value));
        return result;
    }

    private String resolveEndpointUrl() {
        if (containerDetector.isRunningInContainer()) {
            String suffix = config.hostname().orElse(EmbeddedDnsServer.DEFAULT_SUFFIX);
            return "http://" + suffix + ":" + config.port();
        }
        return "http://host.docker.internal:" + config.port();
    }

    private int timeoutMinutes(JobRun run) {
        return run.getTimeout() != null && run.getTimeout() > 0 ? run.getTimeout() : GlueService.DEFAULT_JOB_TIMEOUT_MINUTES;
    }

    private int maxAttempts(Job job) {
        int retries = job.getMaxRetries() != null ? Math.max(0, job.getMaxRetries()) : 0;
        return retries + 1;
    }

    private String attemptDetail(int zeroBasedAttempt, int attempts) {
        if (attempts <= 1) {
            return "Running Glue job";
        }
        return "Running attempt " + (zeroBasedAttempt + 1) + " of " + attempts;
    }

    private String logStreamName(Job job, JobRun run) {
        String jobName = job.getName() != null ? job.getName() : "job";
        return jobName + "/" + run.getId();
    }

    private Integer waitForContainer(String containerId, int timeoutMinutes) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        final Integer[] status = new Integer[1];
        final Throwable[] failure = new Throwable[1];
        try (Closeable ignored = dockerClient.waitContainerCmd(containerId).exec(new ResultCallback.Adapter<WaitResponse>() {
            @Override
            public void onNext(WaitResponse response) {
                Number code = response.getStatusCode();
                status[0] = code != null ? code.intValue() : null;
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                failure[0] = throwable;
                latch.countDown();
            }
        })) {
            boolean completed = latch.await(timeoutMinutes, TimeUnit.MINUTES);
            if (failure[0] != null) {
                LOG.warnv("Error waiting for Glue job container {0}: {1}", containerId, failure[0].getMessage());
                throw new IllegalStateException(
                        "Error waiting for Glue job container: " + failure[0].getMessage(), failure[0]);
            }
            return completed ? status[0] : null;
        } catch (IOException e) {
            return status[0];
        }
    }

    private static String shellQuote(String value) {
        if (value == null) {
            return "''";
        }
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }

    private record ActiveRun(JobRun run, Consumer<JobRun> onUpdate) {
    }

    private record AttemptResult(boolean success, boolean timedOut, String message) {
        static AttemptResult succeeded() {
            return new AttemptResult(true, false, "Succeeded");
        }

        static AttemptResult timedOut(String message) {
            return new AttemptResult(false, true, message);
        }

        static AttemptResult failed(String message) {
            return new AttemptResult(false, false,
                    message == null || message.isBlank() ? "Glue job container failed" : message);
        }
    }
}
