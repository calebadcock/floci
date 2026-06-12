package io.github.hectorvent.floci.services.neptune.proxy;

import org.jboss.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Transparent TCP proxy for a single Neptune DB cluster's Gremlin endpoint.
 *
 * <p>Neptune uses WebSocket over port 8182. The client (botocore) embeds SigV4 credentials
 * in the HTTP Upgrade headers. The backend TinkerPop Gremlin Server accepts plain WebSocket
 * without authentication, so we relay all bytes transparently without inspecting the payload.
 *
 * <p>Uses Java virtual threads for non-blocking I/O.
 */
public class NeptuneGremlinProxy {

    private static final Logger LOG = Logger.getLogger(NeptuneGremlinProxy.class);

    private static final int MAX_HTTP_HEAD_BYTES = 16 * 1024;
    private static final int LOADER_PREFIX_LENGTH = "POST /loader".length();

    private final String clusterId;
    private final String backendHost;
    private final int backendPort;
    private final NeptuneLoaderEndpoint loaderEndpoint;

    private volatile boolean running;
    private ServerSocket serverSocket;

    public NeptuneGremlinProxy(String clusterId, String backendHost, int backendPort,
                               NeptuneLoaderEndpoint loaderEndpoint) {
        this.clusterId = clusterId;
        this.backendHost = backendHost;
        this.backendPort = backendPort;
        this.loaderEndpoint = loaderEndpoint;
    }

    public void start(int proxyPort) throws IOException {
        serverSocket = new ServerSocket(proxyPort);
        running = true;
        Thread.ofVirtual().name("neptune-proxy-accept-" + clusterId).start(this::acceptLoop);
        LOG.infov("Neptune Gremlin proxy started for cluster {0} on port {1} → {2}:{3}",
                clusterId, String.valueOf(proxyPort), backendHost, String.valueOf(backendPort));
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOG.warnv("Error closing proxy server socket for cluster {0}: {1}", clusterId, e.getMessage());
        }
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                Thread.ofVirtual().name("neptune-proxy-conn-" + clusterId).start(() -> relay(client));
            } catch (IOException e) {
                if (running) {
                    LOG.warnv("Accept error for cluster {0}: {1}", clusterId, e.getMessage());
                }
            }
        }
    }

    private void relay(Socket client) {
        try {
            client.setTcpNoDelay(true);
            InputStream clientIn = client.getInputStream();
            // Just enough bytes to recognize a bulk-loader request line; anything else
            // is relayed with the consumed prefix forwarded first
            byte[] prefix = clientIn.readNBytes(LOADER_PREFIX_LENGTH);
            if (loaderEndpoint != null && isLoaderRequest(prefix)) {
                handleLoaderRequest(client, readHttpHead(clientIn, prefix));
                return;
            }
            Socket backend = new Socket(backendHost, backendPort);
            backend.setTcpNoDelay(true);
            backend.getOutputStream().write(prefix);
            backend.getOutputStream().flush();
            bridge(client, backend);
        } catch (IOException e) {
            LOG.debugv("Failed to connect to Gremlin backend for cluster {0}: {1}",
                    clusterId, e.getMessage());
            closeQuietly(client);
        }
    }

    private static boolean isLoaderRequest(byte[] prefix) {
        String requestLine = new String(prefix, StandardCharsets.US_ASCII);
        return requestLine.startsWith("POST /loader") || requestLine.startsWith("GET /loader");
    }

    /**
     * Reads the remainder of the HTTP request head (through {@code \r\n\r\n}) for an
     * intercepted loader request, returning the already-consumed prefix plus the rest.
     */
    private static byte[] readHttpHead(InputStream in, byte[] prefix) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        head.write(prefix, 0, prefix.length);
        int b;
        while (head.size() < MAX_HTTP_HEAD_BYTES && (b = in.read()) != -1) {
            head.write(b);
            byte[] bytes = head.toByteArray();
            int n = bytes.length;
            if (n >= 4 && bytes[n - 4] == '\r' && bytes[n - 3] == '\n'
                    && bytes[n - 2] == '\r' && bytes[n - 1] == '\n') {
                break;
            }
        }
        return head.toByteArray();
    }

    private void handleLoaderRequest(Socket client, byte[] head) throws IOException {
        String headText = new String(head, StandardCharsets.UTF_8);
        int lineEnd = headText.indexOf("\r\n");
        String[] requestLine = (lineEnd < 0 ? headText : headText.substring(0, lineEnd)).split(" ");
        if (requestLine.length < 2) {
            closeQuietly(client);
            return;
        }
        String method = requestLine[0];
        String path = requestLine[1];
        String body = readBody(client.getInputStream(), headText);

        NeptuneLoaderEndpoint.LoaderHttpResponse response =
                loaderEndpoint.handle(method, path, body, backendHost, backendPort);
        byte[] responseBody = response.body().getBytes(StandardCharsets.UTF_8);
        OutputStream out = client.getOutputStream();
        out.write(("HTTP/1.1 " + response.statusCode() + " " + reasonPhrase(response.statusCode())
                + "\r\nContent-Type: application/json\r\nContent-Length: " + responseBody.length
                + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(responseBody);
        out.flush();
        closeQuietly(client);
    }

    private static String readBody(InputStream in, String headText) throws IOException {
        int contentLength = 0;
        for (String line : headText.split("\r\n")) {
            int colon = line.indexOf(':');
            if (colon > 0 && "content-length".equalsIgnoreCase(line.substring(0, colon).strip())) {
                try {
                    contentLength = Integer.parseInt(line.substring(colon + 1).strip());
                } catch (NumberFormatException e) {
                    throw new IOException("Malformed Content-Length in loader request");
                }
            }
        }
        if (contentLength <= 0) {
            return "";
        }
        byte[] body = in.readNBytes(contentLength);
        return new String(body, StandardCharsets.UTF_8);
    }

    private static String reasonPhrase(int statusCode) {
        return switch (statusCode) {
            case 200 -> "OK";
            case 400 -> "Bad Request";
            case 404 -> "Not Found";
            case 405 -> "Method Not Allowed";
            default -> "Error";
        };
    }

    /**
     * Bidirectional byte relay. Relay threads are platform daemon threads; virtual threads
     * for I/O-bound work can stall WebSocket frame delivery under high concurrency.
     */
    private void bridge(Socket client, Socket backend) {
        Thread t1 = Thread.ofPlatform().daemon(true).name("neptune-relay-c2b-" + clusterId)
                .start(() -> pipe(client, backend));
        Thread t2 = Thread.ofPlatform().daemon(true).name("neptune-relay-b2c-" + clusterId)
                .start(() -> pipe(backend, client));
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            closeQuietly(client);
            closeQuietly(backend);
        }
    }

    private static void pipe(Socket from, Socket to) {
        byte[] buf = new byte[8192];
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();
            }
        } catch (IOException ignored) {
            // Normal when either side closes the connection
        }
    }

    private static void closeQuietly(Socket s) {
        try { s.close(); } catch (IOException ignored) {}
    }
}
