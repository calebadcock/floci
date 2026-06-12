package io.github.hectorvent.floci.services.neptune.loader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.services.neptune.proxy.NeptuneGremlinProxy;
import io.github.hectorvent.floci.services.s3.S3Service;
import io.github.hectorvent.floci.services.s3.model.S3Object;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end test of the Neptune bulk loader against a real Gremlin Server container:
 * proxy HTTP interception, S3 CSV translation, and WebSocket writes.
 */
@EnabledIfEnvironmentVariable(named = "FLOCI_NEPTUNE_LOADER_TEST", matches = "1|true|yes")
class NeptuneLoaderDockerIntegrationTest {

    private static final String GREMLIN_IMAGE = "tinkerpop/gremlin-server:3.7.3";
    private static final int BACKEND_PORT = 18182;
    private static final int PROXY_PORT = 18183;
    private static final ObjectMapper mapper = new ObjectMapper();

    private static String containerId;
    private static NeptuneGremlinProxy proxy;

    @BeforeAll
    static void startGremlinServer() throws Exception {
        Assumptions.assumeTrue(isDockerAvailable(), "Docker daemon must be available for Neptune loader tests");
        containerId = docker("create", "-p", BACKEND_PORT + ":8182", GREMLIN_IMAGE);

        // Mirror NeptuneContainerManager: Neptune-style ids accept arbitrary values
        Path properties = Files.createTempFile("tinkergraph-empty", ".properties");
        Files.writeString(properties, """
                gremlin.graph=org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
                gremlin.tinkergraph.vertexIdManager=ANY
                gremlin.tinkergraph.edgeIdManager=ANY
                """);
        Files.setPosixFilePermissions(properties, PosixFilePermissions.fromString("rw-r--r--"));
        docker("cp", properties.toString(),
                containerId + ":/opt/gremlin-server/conf/tinkergraph-empty.properties");
        Files.deleteIfExists(properties);
        docker("start", containerId);
        awaitGremlinServer();

        S3Service s3Service = mock(S3Service.class);
        when(s3Service.getObject(anyString(), anyString())).thenReturn(null);
        when(s3Service.getObject("graph", "vertices.csv")).thenReturn(object("vertices.csv", """
                ~id,~label,name:String,age:Int
                c1,customer,Alice,34
                c2,customer,Bob,41
                """));
        when(s3Service.getObject("graph", "edges.csv")).thenReturn(object("edges.csv", """
                ~id,~from,~to,~label,since:Int
                e1,c1,c2,KNOWS,2020
                """));
        when(s3Service.listObjects(anyString(), anyString(), any(), anyInt())).thenReturn(List.of(
                object("vertices.csv", ""), object("edges.csv", "")));

        proxy = new NeptuneGremlinProxy("loader-test", "localhost", BACKEND_PORT,
                new NeptuneBulkLoader(s3Service, mapper));
        proxy.start(PROXY_PORT);
    }

    @AfterAll
    static void stopEverything() {
        if (proxy != null) {
            proxy.stop();
        }
        if (containerId != null && !containerId.isBlank()) {
            try {
                docker("rm", "-f", containerId);
            } catch (Exception ignored) {
            }
        }
    }

    private static String docker(String... arguments) throws Exception {
        List<String> command = new ArrayList<>(List.of("docker"));
        command.addAll(List.of(arguments));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        process.waitFor(5, TimeUnit.MINUTES);
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
        assertEquals(0, process.exitValue(), "docker " + arguments[0] + " failed: " + output);
        return output;
    }

    @Test
    void bulkLoadWritesVerticesAndEdgesThroughProxy() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpResponse<String> started = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + PROXY_PORT + "/loader"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"source\": \"s3://graph/\", \"format\": \"csv\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, started.statusCode(), started.body());
        String loadId = mapper.readTree(started.body()).path("payload").path("loadId").asText();
        assertFalse(loadId.isBlank());

        JsonNode overall = awaitLoadCompleted(http, loadId);
        assertEquals("LOAD_COMPLETED", overall.path("status").asText(), overall.toString());
        assertEquals(3, overall.path("totalRecords").asInt());
        assertEquals(0, overall.path("insertErrors").asInt());

        try (GremlinTextClient client = new GremlinTextClient("localhost", BACKEND_PORT, mapper)) {
            client.submit("if (g.V().hasLabel('customer').count().next() != 2L) "
                    + "throw new IllegalStateException('expected 2 customers')");
            client.submit("if (g.E().hasLabel('KNOWS').count().next() != 1L) "
                    + "throw new IllegalStateException('expected 1 edge')");
        }
    }

    private static JsonNode awaitLoadCompleted(HttpClient http, String loadId) throws Exception {
        for (int i = 0; i < 300; i++) {
            HttpResponse<String> status = http.send(HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + PROXY_PORT + "/loader/" + loadId))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            JsonNode overall = mapper.readTree(status.body()).path("payload").path("overallStatus");
            if (!"LOAD_IN_PROGRESS".equals(overall.path("status").asText())) {
                return overall;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Load did not finish in time");
    }

    private static void awaitGremlinServer() throws Exception {
        Exception last = null;
        for (int i = 0; i < 120; i++) {
            try (GremlinTextClient client = new GremlinTextClient("localhost", BACKEND_PORT, mapper)) {
                client.submit("g.V().count().next()");
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(1000);
            }
        }
        throw new AssertionError("Gremlin Server did not become ready", last);
    }

    private static S3Object object(String key, String content) {
        S3Object object = new S3Object();
        object.setKey(key);
        object.setData(content.getBytes(StandardCharsets.UTF_8));
        return object;
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
}
