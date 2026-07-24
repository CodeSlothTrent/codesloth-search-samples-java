package SlowLogDemo;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Minimal REST client for Elasticsearch/OpenSearch slow-log capture tests.
 * Uses raw HTTP so one helper works across OpenSearch and multiple Elasticsearch versions.
 */
public class SearchEngineHttp implements AutoCloseable {

    private final HttpClient httpClient;
    private final String baseUrl;

    public SearchEngineHttp(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }

    public void waitForClusterHealth() throws IOException, InterruptedException {
        for (int attempt = 0; attempt < 60; attempt++) {
            HttpResponse<String> response = get("/_cluster/health?wait_for_status=yellow&timeout=5s");
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return;
            }
            Thread.sleep(1_000);
        }
        throw new IllegalStateException("Cluster did not become healthy at " + baseUrl);
    }

    public HttpResponse<String> put(String path, String jsonBody) throws IOException, InterruptedException {
        return send("PUT", path, jsonBody);
    }

    public HttpResponse<String> post(String path, String jsonBody) throws IOException, InterruptedException {
        return send("POST", path, jsonBody);
    }

    public HttpResponse<String> get(String path) throws IOException, InterruptedException {
        return send("GET", path, null);
    }

    public HttpResponse<String> delete(String path) throws IOException, InterruptedException {
        return send("DELETE", path, null);
    }

    private HttpResponse<String> send(String method, String path, String jsonBody)
            throws IOException, InterruptedException {
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + normalizedPath))
                .timeout(Duration.ofMinutes(5))
                .header("Content-Type", "application/json");

        switch (method) {
            case "PUT" -> builder.PUT(HttpRequest.BodyPublishers.ofString(jsonBody == null ? "" : jsonBody));
            case "POST" -> builder.POST(HttpRequest.BodyPublishers.ofString(jsonBody == null ? "" : jsonBody));
            case "GET" -> builder.GET();
            case "DELETE" -> builder.DELETE();
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        }

        HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IllegalStateException(
                    method + " " + normalizedPath + " failed with " + response.statusCode() + ": " + response.body());
        }
        return response;
    }

    @Override
    public void close() {
        // HttpClient has no close hook in Java 21.
    }
}
