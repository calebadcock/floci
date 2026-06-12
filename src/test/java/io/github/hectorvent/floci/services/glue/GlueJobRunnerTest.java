package io.github.hectorvent.floci.services.glue;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.CopyArchiveToContainerCmd;
import com.github.dockerjava.api.command.WaitContainerCmd;
import com.github.dockerjava.api.model.WaitResponse;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.services.glue.model.Job;
import io.github.hectorvent.floci.services.glue.model.JobCommand;
import io.github.hectorvent.floci.services.glue.model.JobRun;
import io.github.hectorvent.floci.services.s3.S3Service;
import io.github.hectorvent.floci.services.s3.model.S3Object;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GlueJobRunnerTest {

    private GlueJobRunner runner;

    @BeforeEach
    void setUp() {
        runner = new GlueJobRunner(null, null, null, null, null, config(false), null);
    }

    @Test
    void imageForUsesGlueVersionSpecificImages() {
        assertEquals("glue4-image", runner.imageFor(job("4.0", "glueetl")));
        assertEquals("glue5-image", runner.imageFor(job("5.0", "glueetl")));
        assertEquals("default-image", runner.imageFor(job(null, "glueetl")));
        assertEquals("default-image", runner.imageFor(job("3.0", "glueetl")));
    }

    @Test
    void sparkCommandDiscoversGlue4AndGlue5Runners() {
        JobRun run = new JobRun();
        run.setArguments(Map.of("--JOB_NAME", "orders"));

        String command = runner.containerCommand(job("4.0", "glueetl"), run);

        assertTrue(command.contains("spark-submit"));
        assertTrue(command.contains("/home/hadoop/aws-glue-libs/bin/gluesparksubmit"));
        assertTrue(command.contains("/home/glue_user/spark/bin/spark-submit"));
        assertTrue(command.contains("/home/glue_user/aws-glue-libs/bin/gluesparksubmit"));
        assertTrue(command.contains("exec \"$FLOCI_GLUE_RUNNER\" '/tmp/floci-glue/script.py' '--JOB_NAME' 'orders'"));
    }

    @Test
    void pythonShellCommandDiscoversPythonRunner() {
        String command = runner.containerCommand(job("5.0", "pythonshell"), new JobRun());

        assertTrue(command.contains("command -v python3"));
        assertTrue(command.contains("command -v python"));
        assertTrue(command.contains("exec \"$FLOCI_GLUE_RUNNER\" '/tmp/floci-glue/script.py'"));
    }

    @Test
    void mockModeRunTransitionsToSucceededWithoutDocker() throws Exception {
        GlueJobRunner mockRunner = new GlueJobRunner(null, null, null, null, null, config(true), null);
        JobRun run = jobRun();
        RecordingConsumer consumer = new RecordingConsumer();

        mockRunner.startJobRun("us-east-1", job("4.0", "glueetl"), run, consumer);
        List<Snapshot> snapshots = consumer.awaitFinished();

        assertEquals("RUNNING", snapshots.get(0).state());
        assertEquals("Preparing Glue job run", snapshots.get(0).stateDetail());
        assertNull(snapshots.get(0).completedOn());
        assertEquals("SUCCEEDED", snapshots.get(1).state());
        assertEquals("Completed by mock Glue runner", snapshots.get(1).stateDetail());
        assertNotNull(snapshots.get(1).completedOn());
        assertEquals("SUCCEEDED", run.getJobRunState());
        assertNull(run.getErrorMessage());
        assertEquals("/aws-glue/jobs/output", run.getLogGroupName());
        assertEquals("job/jr_1", run.getLogStreamName());
        assertNotNull(run.getExecutionTime());
    }

    @Test
    void containerExitZeroTransitionsThroughRunningToSucceeded() throws Exception {
        ContainerHarness harness = containerHarness(exitsWith(0));
        JobRun run = jobRun();
        RecordingConsumer consumer = new RecordingConsumer();

        harness.runner().startJobRun("us-east-1", job("4.0", "glueetl"), run, consumer);
        List<Snapshot> snapshots = consumer.awaitFinished();

        assertEquals("RUNNING", snapshots.get(0).state());
        assertNull(snapshots.get(0).completedOn());
        assertEquals("RUNNING", snapshots.get(1).state());
        assertEquals("Running Glue job", snapshots.get(1).stateDetail());
        assertEquals(0, snapshots.get(1).attempt());
        assertNull(snapshots.get(1).completedOn());
        assertEquals("SUCCEEDED", snapshots.get(2).state());
        assertNotNull(snapshots.get(2).completedOn());
        verify(harness.lifecycleManager(), times(1)).create(any());
    }

    @Test
    void containerNonZeroExitTransitionsToFailed() throws Exception {
        ContainerHarness harness = containerHarness(exitsWith(3));
        JobRun run = jobRun();
        RecordingConsumer consumer = new RecordingConsumer();

        harness.runner().startJobRun("us-east-1", job("4.0", "glueetl"), run, consumer);
        List<Snapshot> snapshots = consumer.awaitFinished();

        Snapshot failed = snapshots.stream().filter(s -> "FAILED".equals(s.state())).findFirst().orElseThrow();
        assertEquals("Container exited with code 3", failed.errorMessage());
        assertNotNull(failed.completedOn());
        assertTrue(snapshots.stream()
                .filter(s -> !"FAILED".equals(s.state()))
                .allMatch(s -> s.completedOn() == null));
        verify(harness.lifecycleManager(), times(1)).create(any());
    }

    @Test
    void dockerDaemonErrorFailsRunInsteadOfTimingOut() throws Exception {
        ContainerHarness harness = containerHarness(
                callback -> callback.onError(new RuntimeException("Docker daemon unavailable")));
        JobRun run = jobRun();
        RecordingConsumer consumer = new RecordingConsumer();

        harness.runner().startJobRun("us-east-1", job("4.0", "glueetl"), run, consumer);
        List<Snapshot> snapshots = consumer.awaitFinished();

        assertEquals("FAILED", run.getJobRunState());
        assertTrue(run.getErrorMessage().contains("Docker daemon unavailable"));
        assertTrue(snapshots.stream().noneMatch(s -> "TIMEOUT".equals(s.state())));
    }

    @Test
    void retriesHonorMaxRetriesAndTrackAttempts() throws Exception {
        ContainerHarness harness = containerHarness(exitsWith(1));
        Job job = job("4.0", "glueetl");
        job.setMaxRetries(2);
        JobRun run = jobRun();
        RecordingConsumer consumer = new RecordingConsumer();

        harness.runner().startJobRun("us-east-1", job, run, consumer);
        List<Snapshot> snapshots = consumer.awaitFinished();

        verify(harness.lifecycleManager(), times(3)).create(any());
        List<String> details = snapshots.stream().map(Snapshot::stateDetail).toList();
        assertTrue(details.contains("Running attempt 1 of 3"));
        assertTrue(details.contains("Container exited with code 1; retrying attempt 2 of 3"));
        assertTrue(details.contains("Running attempt 2 of 3"));
        assertTrue(details.contains("Container exited with code 1; retrying attempt 3 of 3"));
        assertTrue(details.contains("Running attempt 3 of 3"));
        assertEquals("FAILED", run.getJobRunState());
        assertEquals(2, run.getAttempt());
        assertEquals("Container exited with code 1", run.getErrorMessage());
    }

    @Test
    void stoppedRunShortCircuitsBeforeRelaunch() throws Exception {
        ContainerHarness harness = containerHarness(exitsWith(1));
        Job job = job("4.0", "glueetl");
        job.setMaxRetries(2);
        JobRun run = jobRun();
        RecordingConsumer consumer = new RecordingConsumer(updated -> {
            if (updated.getStateDetail() != null && updated.getStateDetail().contains("retrying attempt 2")) {
                updated.setJobRunState("STOPPED");
            }
        });

        harness.runner().startJobRun("us-east-1", job, run, consumer);
        List<Snapshot> snapshots = consumer.awaitFinished();

        verify(harness.lifecycleManager(), times(1)).create(any());
        assertEquals("STOPPED", run.getJobRunState());
        assertTrue(snapshots.stream().noneMatch(s -> "FAILED".equals(s.state()) || "SUCCEEDED".equals(s.state())));
        assertNull(run.getCompletedOn());
    }

    @Test
    void loadScriptRequiresScriptLocation() {
        AwsException missingCommand = loadScriptFailure(runner, null);
        assertEquals("InvalidInputException", missingCommand.getErrorCode());
        assertEquals("Command.ScriptLocation is required", missingCommand.getMessage());
        assertEquals(400, missingCommand.getHttpStatus());

        JobCommand blankLocation = new JobCommand();
        blankLocation.setScriptLocation(" ");
        assertEquals("InvalidInputException", loadScriptFailure(runner, blankLocation).getErrorCode());
    }

    @Test
    void loadScriptRejectsMalformedS3Uri() {
        JobCommand missingKey = new JobCommand();
        missingKey.setScriptLocation("s3://bucket-only");
        AwsException noKey = loadScriptFailure(runner, missingKey);
        assertEquals("InvalidInputException", noKey.getErrorCode());
        assertEquals("Invalid S3 script location: s3://bucket-only", noKey.getMessage());

        JobCommand emptyKey = new JobCommand();
        emptyKey.setScriptLocation("s3://bucket/");
        assertEquals("InvalidInputException", loadScriptFailure(runner, emptyKey).getErrorCode());
    }

    @Test
    void loadScriptMissingS3ObjectThrowsEntityNotFound() {
        S3Service s3Service = mock(S3Service.class);
        GlueJobRunner s3Runner = new GlueJobRunner(null, null, null, null, s3Service, config(false), null);
        JobCommand command = new JobCommand();
        command.setScriptLocation("s3://scripts/missing.py");

        AwsException notFound = loadScriptFailure(s3Runner, command);

        assertEquals("EntityNotFoundException", notFound.getErrorCode());
        assertEquals("Script not found: s3://scripts/missing.py", notFound.getMessage());
        assertEquals(400, notFound.getHttpStatus());
    }

    @Test
    void missingScriptFailsJobRunWithErrorMessage() throws Exception {
        S3Service s3Service = mock(S3Service.class);
        GlueJobRunner s3Runner = new GlueJobRunner(null, null, null, null, s3Service, config(false), null);
        JobRun run = jobRun();
        RecordingConsumer consumer = new RecordingConsumer();

        s3Runner.startJobRun("us-east-1", job("4.0", "glueetl"), run, consumer);
        consumer.awaitFinished();

        assertEquals("FAILED", run.getJobRunState());
        assertEquals("Script not found: s3://scripts/job.py", run.getErrorMessage());
        assertNotNull(run.getCompletedOn());
    }

    @Test
    void containerCommandQuotesSpecialCharactersSafelyForSh() throws Exception {
        JobRun run = new JobRun();
        Map<String, String> arguments = new LinkedHashMap<>();
        arguments.put("--quote", "it's");
        arguments.put("--spaces", "a b  c");
        arguments.put("--dollar", "$HOME");
        arguments.put("--semi", "a;b");
        run.setArguments(arguments);

        String command = runner.containerCommand(job("4.0", "glueetl"), run);

        assertTrue(command.contains("'--quote' 'it'\"'\"'s'"));
        assertTrue(command.contains("'--spaces' 'a b  c'"));
        assertTrue(command.contains("'--dollar' '$HOME'"));
        assertTrue(command.contains("'--semi' 'a;b'"));

        String marker = "exec \"$FLOCI_GLUE_RUNNER\" ";
        String quotedArguments = command.substring(command.indexOf(marker) + marker.length());
        assertEquals(List.of("/tmp/floci-glue/script.py",
                        "--quote", "it's",
                        "--spaces", "a b  c",
                        "--dollar", "$HOME",
                        "--semi", "a;b"),
                shellTokens(quotedArguments));
    }

    @Test
    void containerCommandEmitsBlankArgumentValuesAsKeyOnly() {
        JobRun run = new JobRun();
        Map<String, String> arguments = new LinkedHashMap<>();
        arguments.put("--empty", "");
        arguments.put("--whitespace", "  ");
        arguments.put("--null", null);
        run.setArguments(arguments);

        String command = runner.containerCommand(job(null, "glueetl"), run);

        assertTrue(command.endsWith("'/tmp/floci-glue/script.py' '--empty' '--whitespace' '--null'"));
    }

    private static List<String> shellTokens(String quotedArguments) throws Exception {
        Process process = new ProcessBuilder("sh", "-c", "printf '%s\\n' " + quotedArguments).start();
        List<String> lines;
        try (BufferedReader reader = process.inputReader()) {
            lines = reader.lines().toList();
        }
        assertEquals(0, process.waitFor());
        return lines;
    }

    private static AwsException loadScriptFailure(GlueJobRunner runner, JobCommand command) {
        try {
            Method loadScript = GlueJobRunner.class.getDeclaredMethod("loadScript", JobCommand.class);
            loadScript.setAccessible(true);
            loadScript.invoke(runner, command);
            return fail("Expected AwsException from loadScript");
        } catch (InvocationTargetException e) {
            return assertInstanceOf(AwsException.class, e.getCause());
        } catch (ReflectiveOperationException e) {
            return fail(e);
        }
    }

    private ContainerHarness containerHarness(Consumer<ResultCallback.Adapter<WaitResponse>> waitBehavior) {
        DockerClient dockerClient = mock(DockerClient.class);
        ContainerBuilder containerBuilder = mock(ContainerBuilder.class);
        ContainerBuilder.Builder builder = mock(ContainerBuilder.Builder.class, RETURNS_SELF);
        when(containerBuilder.newContainer(anyString())).thenReturn(builder);

        ContainerLifecycleManager lifecycleManager = mock(ContainerLifecycleManager.class);
        when(lifecycleManager.create(any())).thenReturn("container-1");

        CopyArchiveToContainerCmd copyCmd = mock(CopyArchiveToContainerCmd.class, RETURNS_SELF);
        when(dockerClient.copyArchiveToContainerCmd("container-1")).thenReturn(copyCmd);

        WaitContainerCmd waitCmd = mock(WaitContainerCmd.class);
        when(dockerClient.waitContainerCmd("container-1")).thenReturn(waitCmd);
        when(waitCmd.exec(any())).thenAnswer(invocation -> {
            ResultCallback.Adapter<WaitResponse> callback = invocation.getArgument(0);
            waitBehavior.accept(callback);
            return callback;
        });

        S3Service s3Service = mock(S3Service.class);
        S3Object script = new S3Object();
        script.setData("print('hello')".getBytes(StandardCharsets.UTF_8));
        when(s3Service.getObject("scripts", "job.py")).thenReturn(script);

        GlueJobRunner containerRunner = new GlueJobRunner(dockerClient, containerBuilder, lifecycleManager,
                null, s3Service, config(false), mock(ContainerDetector.class));
        return new ContainerHarness(containerRunner, lifecycleManager);
    }

    private static Consumer<ResultCallback.Adapter<WaitResponse>> exitsWith(int exitCode) {
        return callback -> {
            WaitResponse response = mock(WaitResponse.class);
            when(response.getStatusCode()).thenReturn(exitCode);
            callback.onNext(response);
            callback.onComplete();
        };
    }

    private static JobRun jobRun() {
        JobRun run = new JobRun();
        run.setId("jr_1");
        run.setJobName("job");
        run.setJobRunState("STARTING");
        run.setTimeout(5);
        return run;
    }

    private static Job job(String glueVersion, String commandName) {
        JobCommand command = new JobCommand();
        command.setName(commandName);
        command.setScriptLocation("s3://scripts/job.py");
        Job job = new Job();
        job.setName("job");
        job.setRole("role");
        job.setGlueVersion(glueVersion);
        job.setCommand(command);
        return job;
    }

    private static EmulatorConfig config(boolean mock) {
        EmulatorConfig.GlueServiceConfig glue = proxy(EmulatorConfig.GlueServiceConfig.class, Map.of(
                "defaultJobImage", "default-image",
                "glue4JobImage", "glue4-image",
                "glue5JobImage", "glue5-image",
                "dockerNetwork", Optional.empty(),
                "mock", mock
        ));
        EmulatorConfig.ServicesConfig services = proxy(EmulatorConfig.ServicesConfig.class, Map.of("glue", glue));
        return proxy(EmulatorConfig.class, Map.of(
                "services", services,
                "hostname", Optional.empty(),
                "port", 4566
        ));
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, Map<String, Object> values) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type },
                (proxy, method, args) -> {
                    if (values.containsKey(method.getName())) {
                        return values.get(method.getName());
                    }
                    if ("toString".equals(method.getName())) {
                        return type.getSimpleName() + values;
                    }
                    if ("hashCode".equals(method.getName())) {
                        return System.identityHashCode(proxy);
                    }
                    if ("equals".equals(method.getName())) {
                        return proxy == args[0];
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private record ContainerHarness(GlueJobRunner runner, ContainerLifecycleManager lifecycleManager) {
    }

    private record Snapshot(String state, String stateDetail, String errorMessage,
                            Instant completedOn, Integer attempt) {
    }

    private static final class RecordingConsumer implements Consumer<JobRun> {
        private final List<Snapshot> snapshots = new CopyOnWriteArrayList<>();
        private final CountDownLatch finished = new CountDownLatch(1);
        private final Consumer<JobRun> hook;

        RecordingConsumer() {
            this(run -> { });
        }

        RecordingConsumer(Consumer<JobRun> hook) {
            this.hook = hook;
        }

        @Override
        public void accept(JobRun run) {
            snapshots.add(new Snapshot(run.getJobRunState(), run.getStateDetail(),
                    run.getErrorMessage(), run.getCompletedOn(), run.getAttempt()));
            hook.accept(run);
            if (run.getExecutionTime() != null) {
                finished.countDown();
            }
        }

        List<Snapshot> awaitFinished() throws InterruptedException {
            assertTrue(finished.await(10, TimeUnit.SECONDS), "Glue job run did not finish");
            return snapshots;
        }
    }
}
