package io.github.hectorvent.floci.services.neptune.proxy;

/**
 * Handles Neptune bulk-loader HTTP requests intercepted by {@link NeptuneGremlinProxy}
 * on a cluster's Gremlin endpoint.
 */
public interface NeptuneLoaderEndpoint {

    LoaderHttpResponse handle(String method, String path, String body,
                              String backendHost, int backendPort);

    record LoaderHttpResponse(int statusCode, String body) {}
}
