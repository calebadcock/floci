package io.github.hectorvent.floci.services.neptune.loader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Minimal Gremlin Server client speaking the JSON text-frame protocol over WebSocket.
 *
 * <p>Sends one {@code eval} request at a time and blocks for the terminal response frame
 * (status code other than 206 PARTIAL_CONTENT). Kept dependency-free by using the JDK
 * {@link WebSocket} client instead of the TinkerPop driver.
 */
final class GremlinTextClient implements AutoCloseable {

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final ObjectMapper objectMapper;
    private final WebSocket webSocket;
    private final Listener listener = new Listener();

    GremlinTextClient(String host, int port, ObjectMapper objectMapper) throws IOException {
        this.objectMapper = objectMapper;
        try {
            this.webSocket = HttpClient.newHttpClient().newWebSocketBuilder()
                    .connectTimeout(REQUEST_TIMEOUT)
                    .buildAsync(URI.create("ws://" + host + ":" + port + "/gremlin"), listener)
                    .get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while connecting to Gremlin server", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Could not connect to Gremlin server at " + host + ":" + port, e);
        }
    }

    synchronized void submit(String gremlin) throws IOException {
        String requestId = UUID.randomUUID().toString();
        String request;
        try {
            request = objectMapper.writeValueAsString(objectMapper.createObjectNode()
                    .put("requestId", requestId)
                    .put("op", "eval")
                    .put("processor", "")
                    .set("args", objectMapper.createObjectNode()
                            .put("gremlin", gremlin)
                            .put("language", "gremlin-groovy")));
        } catch (IOException e) {
            throw new IOException("Could not serialize Gremlin request", e);
        }
        listener.expectResponse(requestId);
        webSocket.sendText(request, true);
        JsonNode response = listener.awaitResponse();
        int code = response.path("status").path("code").asInt();
        if (code != 200 && code != 204) {
            throw new IOException("Gremlin request failed with status " + code + ": "
                    + response.path("status").path("message").asText());
        }
    }

    @Override
    public void close() {
        webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done");
        webSocket.abort();
    }

    private final class Listener implements WebSocket.Listener {

        private final StringBuilder buffer = new StringBuilder();
        private volatile CompletableFuture<JsonNode> pending = new CompletableFuture<>();
        private volatile String expectedRequestId;

        void expectResponse(String requestId) {
            expectedRequestId = requestId;
            pending = new CompletableFuture<>();
        }

        JsonNode awaitResponse() throws IOException {
            try {
                return pending.get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for Gremlin response", e);
            } catch (ExecutionException | TimeoutException e) {
                throw new IOException("Gremlin request failed: " + e.getMessage(), e);
            }
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String message = buffer.toString();
                buffer.setLength(0);
                try {
                    JsonNode response = objectMapper.readTree(message);
                    // Ignore stale frames from a previously timed-out request
                    if (response.path("requestId").asText().equals(expectedRequestId)
                            && response.path("status").path("code").asInt() != 206) {
                        pending.complete(response);
                    }
                } catch (IOException e) {
                    pending.completeExceptionally(e);
                }
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            pending.completeExceptionally(error);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            pending.completeExceptionally(new IOException("Gremlin connection closed: " + reason));
            return null;
        }
    }
}
