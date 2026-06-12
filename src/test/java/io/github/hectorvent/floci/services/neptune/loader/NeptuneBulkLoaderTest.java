package io.github.hectorvent.floci.services.neptune.loader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.services.neptune.proxy.NeptuneLoaderEndpoint.LoaderHttpResponse;
import io.github.hectorvent.floci.services.s3.S3Service;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NeptuneBulkLoaderTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private S3Service s3Service;
    private NeptuneBulkLoader loader;

    @BeforeEach
    void setUp() {
        s3Service = mock(S3Service.class);
        loader = new NeptuneBulkLoader(s3Service, mapper);
    }

    @Test
    void postLoaderReturnsLoadId() throws Exception {
        LoaderHttpResponse response = loader.handle("POST", "/loader",
                "{\"source\": \"s3://graph/vertices.csv\", \"format\": \"csv\"}", "localhost", 1);

        assertEquals(200, response.statusCode());
        JsonNode body = mapper.readTree(response.body());
        assertEquals("200 OK", body.path("status").asText());
        assertFalse(body.path("payload").path("loadId").asText().isBlank());
    }

    @Test
    void postLoaderWithoutS3SourceIsRejected() throws Exception {
        LoaderHttpResponse response = loader.handle("POST", "/loader",
                "{\"source\": \"http://example.com/data.csv\"}", "localhost", 1);

        assertEquals(400, response.statusCode());
        assertEquals("InvalidParameterException", mapper.readTree(response.body()).path("code").asText());
    }

    @Test
    void postLoaderWithUnsupportedFormatIsRejected() throws Exception {
        LoaderHttpResponse response = loader.handle("POST", "/loader",
                "{\"source\": \"s3://graph/\", \"format\": \"turtle\"}", "localhost", 1);

        assertEquals(400, response.statusCode());
        assertTrue(mapper.readTree(response.body()).path("detailedMessage").asText().contains("turtle"));
    }

    @Test
    void postLoaderWithMalformedBodyIsRejected() throws Exception {
        LoaderHttpResponse response = loader.handle("POST", "/loader", "{not json", "localhost", 1);

        assertEquals(400, response.statusCode());
        assertEquals("InvalidParameterException", mapper.readTree(response.body()).path("code").asText());
    }

    @Test
    void getLoaderStatusForUnknownLoadIs404() throws Exception {
        LoaderHttpResponse response = loader.handle("GET", "/loader/no-such-load", "", "localhost", 1);

        assertEquals(404, response.statusCode());
        assertEquals("LoadNotFoundException", mapper.readTree(response.body()).path("code").asText());
    }

    @Test
    void getLoaderListsKnownLoadIds() throws Exception {
        when(s3Service.getObject(anyString(), anyString())).thenReturn(null);
        when(s3Service.listObjects(anyString(), anyString(), any(), anyInt())).thenReturn(List.of());

        LoaderHttpResponse started = loader.handle("POST", "/loader",
                "{\"source\": \"s3://graph/empty/\"}", "localhost", 1);
        String loadId = mapper.readTree(started.body()).path("payload").path("loadId").asText();

        LoaderHttpResponse response = loader.handle("GET", "/loader", "", "localhost", 1);
        JsonNode ids = mapper.readTree(response.body()).path("payload").path("loadIds");

        assertTrue(ids.isArray());
        boolean found = false;
        for (JsonNode id : ids) {
            found |= loadId.equals(id.asText());
        }
        assertTrue(found, "loadIds must contain " + loadId);
    }

    @Test
    void loadWithNoMatchingS3ObjectsEndsInS3ReadError() throws Exception {
        when(s3Service.getObject(anyString(), anyString())).thenReturn(null);
        when(s3Service.listObjects(anyString(), anyString(), any(), anyInt())).thenReturn(List.of());

        LoaderHttpResponse started = loader.handle("POST", "/loader",
                "{\"source\": \"s3://graph/missing/\"}", "localhost", 1);
        String loadId = mapper.readTree(started.body()).path("payload").path("loadId").asText();

        String status = awaitTerminalStatus(loadId);
        assertEquals("LOAD_S3_READ_ERROR", status);
    }

    @Test
    void prefixSourceToleratesNoSuchKeyFromExactLookup() throws Exception {
        // S3Service.getObject throws NoSuchKey for missing keys; a prefix source
        // must fall through to listObjects instead of failing the load
        when(s3Service.getObject(anyString(), anyString()))
                .thenThrow(new AwsException("NoSuchKey", "The specified key does not exist.", 404));
        when(s3Service.listObjects(anyString(), anyString(), any(), anyInt())).thenReturn(List.of());

        LoaderHttpResponse started = loader.handle("POST", "/loader",
                "{\"source\": \"s3://graph/orders/\"}", "localhost", 1);
        String loadId = mapper.readTree(started.body()).path("payload").path("loadId").asText();

        String status = awaitTerminalStatus(loadId);
        assertEquals("LOAD_S3_READ_ERROR", status);
    }

    @Test
    void unsupportedMethodIs405() {
        LoaderHttpResponse response = loader.handle("DELETE", "/loader/abc", "", "localhost", 1);

        assertEquals(405, response.statusCode());
    }

    private String awaitTerminalStatus(String loadId) throws Exception {
        for (int i = 0; i < 100; i++) {
            LoaderHttpResponse response = loader.handle("GET", "/loader/" + loadId, "", "localhost", 1);
            String status = mapper.readTree(response.body())
                    .path("payload").path("overallStatus").path("status").asText();
            if (!"LOAD_IN_PROGRESS".equals(status)) {
                return status;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Load did not reach a terminal status");
    }
}
