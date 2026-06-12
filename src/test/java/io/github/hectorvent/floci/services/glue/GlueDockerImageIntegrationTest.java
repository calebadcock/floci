package io.github.hectorvent.floci.services.glue;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.services.glue.model.Job;
import io.github.hectorvent.floci.services.glue.model.JobCommand;
import io.github.hectorvent.floci.services.glue.model.JobRun;
import io.smallrye.config.WithDefault;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfEnvironmentVariable(named = "FLOCI_GLUE_DOCKER_IMAGE_TEST", matches = "1|true|yes")
class GlueDockerImageIntegrationTest {

    private static final Duration DOCKER_TIMEOUT = Duration.ofMinutes(20);
    private static final String SCRIPT_SETUP = """
            mkdir -p /tmp/floci-glue
            printf 'import os\\nprint("floci-glue-ok", os.environ.get("GLUE_JOB_NAME"), os.environ.get("GLUE_JOB_RUN_ID"))\\n' > /tmp/floci-glue/script.py
            """;

    private static GlueJobRunner runner;

    @BeforeAll
    static void requireDocker() {
        Assumptions.assumeTrue(isDockerAvailable(), "Docker daemon must be available for Glue image integration tests");
        runner = newRunner();
    }

    @Test
    void glue4ImageRunsPythonShellCommand() throws Exception {
        assertProbeSucceeds(job("4.0", "pythonshell"));
    }

    @Test
    void glue4ImageRunsSparkCommand() throws Exception {
        assertProbeSucceeds(job("4.0", "glueetl"));
    }

    @Test
    void glue5ImageRunsPythonShellCommand() throws Exception {
        assertProbeSucceeds(job("5.0", "pythonshell"));
    }

    @Test
    void glue5ImageRunsSparkCommand() throws Exception {
        assertProbeSucceeds(job("5.0", "glueetl"));
    }

    private static void assertProbeSucceeds(Job job) throws IOException, InterruptedException {
        ProbeResult result = runImageProbe(runner.imageFor(job),
                SCRIPT_SETUP + runner.containerCommand(job, new JobRun()));

        assertEquals(0, result.exitCode(), result.output());
        assertTrue(result.output().contains("floci-glue-ok probe jr_probe"), result.output());
    }

    private static ProbeResult runImageProbe(String image, String command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(
                "docker", "run", "--rm",
                "-e", "GLUE_JOB_NAME=probe",
                "-e", "GLUE_JOB_RUN_ID=jr_probe",
                "--entrypoint", "sh", image, "-lc", command)
                .redirectErrorStream(true)
                .start();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        Thread drainer = Thread.ofVirtual().start(() -> {
            try {
                process.getInputStream().transferTo(buffer);
            } catch (IOException ignored) {
            }
        });
        boolean finished = process.waitFor(DOCKER_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        drainer.join();
        String output = buffer.toString(StandardCharsets.UTF_8);
        if (!finished) {
            return new ProbeResult(124, output + "\nTimed out waiting for Docker image probe");
        }
        return new ProbeResult(process.exitValue(), output);
    }

    private static GlueJobRunner newRunner() {
        EmulatorConfig.GlueServiceConfig glue = proxy(EmulatorConfig.GlueServiceConfig.class, Map.of(
                "defaultJobImage", defaultImage("defaultJobImage"),
                "glue4JobImage", defaultImage("glue4JobImage"),
                "glue5JobImage", defaultImage("glue5JobImage"),
                "dockerNetwork", Optional.empty(),
                "mock", false
        ));
        EmulatorConfig.ServicesConfig services = proxy(EmulatorConfig.ServicesConfig.class, Map.of("glue", glue));
        EmulatorConfig config = proxy(EmulatorConfig.class, Map.of(
                "services", services,
                "hostname", Optional.empty(),
                "port", 4566
        ));
        return new GlueJobRunner(null, null, null, null, null, config, new ContainerDetector());
    }

    private static String defaultImage(String methodName) {
        try {
            return EmulatorConfig.GlueServiceConfig.class.getMethod(methodName)
                    .getAnnotation(WithDefault.class).value();
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Job job(String glueVersion, String commandName) {
        JobCommand command = new JobCommand();
        command.setName(commandName);
        command.setScriptLocation("s3://scripts/job.py");
        Job job = new Job();
        job.setName("probe");
        job.setRole("role");
        job.setGlueVersion(glueVersion);
        job.setCommand(command);
        return job;
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

    private static boolean isDockerAvailable() {
        try {
            Process process = new ProcessBuilder("docker", "version", "--format", "{{.Server.Version}}")
                    .redirectErrorStream(true)
                    .start();
            return process.waitFor(10, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    private record ProbeResult(int exitCode, String output) {}
}
