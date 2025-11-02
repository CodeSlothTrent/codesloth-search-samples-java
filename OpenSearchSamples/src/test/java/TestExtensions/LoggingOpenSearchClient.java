package TestExtensions;

import TestInfrastructure.OpenSearchRequestLogger;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.io.IOException;
import java.util.function.Function;

/**
 * A wrapper around OpenSearchClient that automatically logs search requests and responses.
 * This is useful for debugging and capturing examples for documentation.
 */
public class LoggingOpenSearchClient {
    private final OpenSearchClient client;

    public LoggingOpenSearchClient(OpenSearchClient client) {
        this.client = client;
    }

    /**
     * Gets the underlying OpenSearchClient for operations that aren't explicitly wrapped.
     * 
     * @return the wrapped OpenSearchClient
     */
    public OpenSearchClient getClient() {
        return client;
    }

    /**
     * Performs a search operation with automatic request/response logging.
     */
    public <T> SearchResponse<T> search(Function<SearchRequest.Builder, SearchRequest.Builder> fn, Class<T> clazz) throws IOException {
        var builder = new SearchRequest.Builder();
        var request = fn.apply(builder).build();
        OpenSearchRequestLogger.LogRequestJson(request);
        SearchResponse<T> response = client.search(request, clazz);
        OpenSearchRequestLogger.LogResponseJson(client, response);
        return response;
    }
}
