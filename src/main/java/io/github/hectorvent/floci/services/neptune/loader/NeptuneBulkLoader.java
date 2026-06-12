package io.github.hectorvent.floci.services.neptune.loader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.services.neptune.proxy.NeptuneLoaderEndpoint;
import io.github.hectorvent.floci.services.s3.S3Service;
import io.github.hectorvent.floci.services.s3.model.S3Object;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Emulates the Neptune bulk loader HTTP API ({@code POST /loader}, {@code GET /loader/{loadId}})
 * exposed on each cluster's Gremlin endpoint.
 *
 * <p>Loads Gremlin CSV files from the emulated S3 service and writes vertices and edges to the
 * cluster's Gremlin Server backend. Vertex files are loaded before edge files so that edges can
 * reference vertices from the same load.
 */
@ApplicationScoped
public class NeptuneBulkLoader implements NeptuneLoaderEndpoint {

    private static final Logger LOG = Logger.getLogger(NeptuneBulkLoader.class);

    private final S3Service s3Service;
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<String, LoadJob> loads = new ConcurrentHashMap<>();

    @Inject
    public NeptuneBulkLoader(S3Service s3Service, ObjectMapper objectMapper) {
        this.s3Service = s3Service;
        this.objectMapper = objectMapper;
    }

    @Override
    public LoaderHttpResponse handle(String method, String path, String body,
                                     String backendHost, int backendPort) {
        if ("POST".equals(method) && "/loader".equals(stripQuery(path))) {
            return startLoad(body, backendHost, backendPort);
        }
        if ("GET".equals(method)) {
            String remainder = stripQuery(path).substring("/loader".length());
            if (remainder.isEmpty() || "/".equals(remainder)) {
                return listLoads();
            }
            return loadStatus(remainder.substring(1));
        }
        return error(405, "MethodNotAllowedException", "Unsupported loader request: " + method + " " + path);
    }

    private LoaderHttpResponse startLoad(String body, String backendHost, int backendPort) {
        JsonNode request;
        try {
            request = objectMapper.readTree(body == null || body.isBlank() ? "{}" : body);
        } catch (IOException e) {
            return error(400, "InvalidParameterException", "Malformed loader request body");
        }
        String source = request.path("source").asText(null);
        if (source == null || !source.startsWith("s3://")) {
            return error(400, "InvalidParameterException", "source must be an s3:// URI");
        }
        String format = request.path("format").asText("csv");
        if (!"csv".equalsIgnoreCase(format)) {
            return error(400, "InvalidParameterException", "Unsupported format: " + format
                    + " (only csv is supported)");
        }

        LoadJob job = new LoadJob(UUID.randomUUID().toString(), source);
        loads.put(job.loadId, job);
        Thread.ofVirtual().name("neptune-loader-" + job.loadId)
                .start(() -> runLoad(job, backendHost, backendPort));

        ObjectNode payload = objectMapper.createObjectNode().put("loadId", job.loadId);
        return ok(payload);
    }

    private LoaderHttpResponse listLoads() {
        ObjectNode payload = objectMapper.createObjectNode();
        ArrayNode ids = payload.putArray("loadIds");
        loads.keySet().forEach(ids::add);
        return ok(payload);
    }

    private LoaderHttpResponse loadStatus(String loadId) {
        LoadJob job = loads.get(loadId);
        if (job == null) {
            return error(404, "LoadNotFoundException", "Load not found: " + loadId);
        }
        ObjectNode overall = objectMapper.createObjectNode()
                .put("fullUri", job.source)
                .put("runNumber", 1)
                .put("retryNumber", 0)
                .put("status", job.status)
                .put("totalTimeSpent", job.totalTimeSpentSeconds())
                .put("totalRecords", job.totalRecords)
                .put("totalDuplicates", 0)
                .put("parsingErrors", job.parsingErrors)
                .put("datatypeMismatchErrors", 0)
                .put("insertErrors", job.insertErrors);
        ObjectNode payload = objectMapper.createObjectNode();
        payload.putArray("feedCount").add(objectMapper.createObjectNode().put(job.status, 1));
        payload.set("overallStatus", overall);
        return ok(payload);
    }

    private void runLoad(LoadJob job, String backendHost, int backendPort) {
        try {
            List<S3Object> sources = resolveSourceObjects(job.source);
            if (sources.isEmpty()) {
                job.fail("LOAD_S3_READ_ERROR");
                return;
            }
            List<NeptuneLoaderCsv.ParsedFile> files = new ArrayList<>();
            for (S3Object object : sources) {
                files.add(NeptuneLoaderCsv.parse(new String(object.getData(), StandardCharsets.UTF_8)));
            }
            // Vertices first so edge files in the same load can reference them
            files.sort(Comparator.comparing(NeptuneLoaderCsv.ParsedFile::edges));
            try (GremlinTextClient client = new GremlinTextClient(backendHost, backendPort, objectMapper)) {
                for (NeptuneLoaderCsv.ParsedFile file : files) {
                    job.parsingErrors += file.parsingErrors();
                    for (String script : file.rowScripts()) {
                        try {
                            client.submit(script);
                            job.totalRecords++;
                        } catch (IOException e) {
                            LOG.debugv("Neptune load {0} insert failed: {1}", job.loadId, e.getMessage());
                            job.insertErrors++;
                        }
                    }
                }
            }
            job.complete(job.insertErrors == 0 ? "LOAD_COMPLETED" : "LOAD_FAILED");
            LOG.infov("Neptune load {0} finished: {1} records, {2} parsing errors, {3} insert errors",
                    job.loadId, String.valueOf(job.totalRecords), String.valueOf(job.parsingErrors),
                    String.valueOf(job.insertErrors));
        } catch (Exception e) {
            LOG.warnv("Neptune load {0} failed: {1}", job.loadId, e.getMessage());
            job.fail("LOAD_FAILED");
        }
    }

    private List<S3Object> resolveSourceObjects(String source) {
        String withoutScheme = source.substring("s3://".length());
        int slash = withoutScheme.indexOf('/');
        String bucket = slash < 0 ? withoutScheme : withoutScheme.substring(0, slash);
        String key = slash < 0 ? "" : withoutScheme.substring(slash + 1);

        S3Object exact = key.isEmpty() ? null : getObjectIfExists(bucket, key);
        if (exact != null) {
            return List.of(exact);
        }
        List<S3Object> objects = new ArrayList<>();
        for (S3Object object : s3Service.listObjects(bucket, key, null, 1000)) {
            S3Object full = getObjectIfExists(bucket, object.getKey());
            if (full != null) {
                objects.add(full);
            }
        }
        return objects;
    }

    private S3Object getObjectIfExists(String bucket, String key) {
        try {
            S3Object object = s3Service.getObject(bucket, key);
            return object != null && object.getData() != null ? object : null;
        } catch (AwsException e) {
            return null;
        }
    }

    private LoaderHttpResponse ok(ObjectNode payload) {
        ObjectNode response = objectMapper.createObjectNode().put("status", "200 OK");
        response.set("payload", payload);
        return new LoaderHttpResponse(200, response.toString());
    }

    private LoaderHttpResponse error(int statusCode, String code, String message) {
        ObjectNode response = objectMapper.createObjectNode()
                .put("detailedMessage", message)
                .put("requestId", UUID.randomUUID().toString())
                .put("code", code);
        return new LoaderHttpResponse(statusCode, response.toString());
    }

    private static String stripQuery(String path) {
        int question = path.indexOf('?');
        return question < 0 ? path : path.substring(0, question);
    }

    private static final class LoadJob {
        final String loadId;
        final String source;
        final long startedAt = System.currentTimeMillis();
        volatile String status = "LOAD_IN_PROGRESS";
        volatile long finishedAt;
        volatile int totalRecords;
        volatile int parsingErrors;
        volatile int insertErrors;

        LoadJob(String loadId, String source) {
            this.loadId = loadId;
            this.source = source;
        }

        void complete(String finalStatus) {
            status = finalStatus;
            finishedAt = System.currentTimeMillis();
        }

        void fail(String finalStatus) {
            complete(finalStatus);
        }

        int totalTimeSpentSeconds() {
            long end = finishedAt > 0 ? finishedAt : System.currentTimeMillis();
            return (int) ((end - startedAt) / 1000);
        }
    }
}
