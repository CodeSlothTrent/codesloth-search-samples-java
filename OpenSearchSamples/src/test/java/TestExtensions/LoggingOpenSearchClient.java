package TestExtensions;

import TestInfrastructure.OpenSearchRequestLogger;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.CountRequest;
import org.opensearch.client.opensearch.core.CountResponse;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.TermvectorsRequest;
import org.opensearch.client.opensearch.core.TermvectorsResponse;
import org.opensearch.client.opensearch.indices.GetMappingRequest;
import org.opensearch.client.opensearch.indices.GetMappingResponse;
import org.opensearch.client.opensearch.indices.IndicesStatsRequest;
import org.opensearch.client.opensearch.indices.IndicesStatsResponse;

import java.io.IOException;
import java.util.function.Function;

/**
 * A wrapper around OpenSearchClient that automatically logs search requests and responses.
 * This is useful for debugging and capturing examples for documentation.
 * Each instance is scoped to a test class.
 */
public class LoggingOpenSearchClient {
    private final OpenSearchClient client;
    private final OpenSearchRequestLogger logger;

    public LoggingOpenSearchClient(OpenSearchClient client, String testClassName, boolean enableCapture) {
        this.client = client;
        this.logger = new OpenSearchRequestLogger(testClassName, enableCapture);
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
     * Gets the logger instance for this test class.
     * 
     * @return the logger instance
     */
    public OpenSearchRequestLogger getLogger() {
        return logger;
    }

    /**
     * Performs a search operation with automatic request/response logging.
     */
    public <T> SearchResponse<T> search(Function<SearchRequest.Builder, SearchRequest.Builder> fn, Class<T> clazz) throws IOException {
        var builder = new SearchRequest.Builder();
        var request = fn.apply(builder).build();
        logger.logRequest(client, request);
        SearchResponse<T> response = client.search(request, clazz);
        logger.logResponse(client, response);
        return response;
    }

    /**
     * Performs a termvectors operation with automatic request/response logging.
     */
    public TermvectorsResponse termvectors(Function<TermvectorsRequest.Builder<Object>, TermvectorsRequest.Builder<Object>> fn) throws IOException {
        var builder = new TermvectorsRequest.Builder<>();
        var request = fn.apply(builder).build();
        logger.logRequest(client, request);
        TermvectorsResponse response = client.termvectors((TermvectorsRequest<Object>) request);
        logger.logResponse(client, response);
        return response;
    }

    /**
     * Performs a count operation with automatic request/response logging.
     */
    public CountResponse count(Function<CountRequest.Builder, CountRequest.Builder> fn) throws IOException {
        var builder = new CountRequest.Builder();
        var request = fn.apply(builder).build();
        
        // Log the request
        logger.logRequest(client, request);
        
        // Perform the request
        CountResponse response = client.count(request);
        
        // Log the response
        logger.logResponse(client, response);
        
        return response;
    }

    /**
     * Performs a get operation with automatic request/response logging.
     */
    public <T> GetResponse<T> get(Function<GetRequest.Builder, GetRequest.Builder> fn, Class<T> clazz) throws IOException {
        var builder = new GetRequest.Builder();
        var request = fn.apply(builder).build();
        
        // Log the request
        logger.logRequest(client, request);
        
        // Perform the request
        GetResponse<T> response = client.get(request, clazz);
        
        // Log the response
        logger.logResponse(client, response);
        
        return response;
    }

    /**
     * Performs a get mapping operation with automatic request/response logging.
     */
    public GetMappingResponse getMapping(Function<GetMappingRequest.Builder, GetMappingRequest.Builder> fn) throws IOException {
        var builder = new GetMappingRequest.Builder();
        var request = fn.apply(builder).build();
        
        // Log the request
        logger.logRequest(client, request);
        
        // Perform the request
        GetMappingResponse response = client.indices().getMapping(request);
        
        // Log the response
        logger.logResponse(client, response);
        
        return response;
    }

    /**
     * Performs a stats API call for the specified index with automatic request/response logging.
     * The stats API returns the actual Lucene document count, including nested documents.
     * 
     * @param indexName The name of the index to get stats for
     * @return The stats API response
     * @throws IOException If the request fails
     */
    public IndicesStatsResponse stats(String indexName) throws IOException {
        // Use the client's indices().stats() method
        IndicesStatsRequest statsRequest = IndicesStatsRequest.of(s -> s
                .index(indexName)
        );
        
        // Log the request
        logger.logRequest(client, statsRequest);
        
        // Perform the request
        IndicesStatsResponse response = client.indices().stats(statsRequest);
        
        // Log the response
        logger.logResponse(client, response);
        
        return response;
    }
}
