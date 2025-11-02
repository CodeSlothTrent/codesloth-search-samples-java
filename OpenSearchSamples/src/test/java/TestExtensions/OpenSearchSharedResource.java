package TestExtensions;

import org.opensearch.client.opensearch.OpenSearchClient;

public class OpenSearchSharedResource {
    private OpenSearchClient openSearchClient;
    private LoggingOpenSearchClient loggingOpenSearchClient;

    public OpenSearchSharedResource(OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
        this.loggingOpenSearchClient = new LoggingOpenSearchClient(openSearchClient);
    }

    public OpenSearchClient getOpenSearchClient() {
        return openSearchClient;
    }
    
    /**
     * Gets a LoggingOpenSearchClient that automatically logs requests and responses
     * for all operations. This is useful for debugging and capturing examples for documentation.
     * 
     * @return a LoggingOpenSearchClient wrapper around the underlying OpenSearchClient
     */
    public LoggingOpenSearchClient getLoggingOpenSearchClient() {
        return loggingOpenSearchClient;
    }
    
    /**
     * Gets the URL for accessing OpenSearch Dashboards.
     * 
     * @return the Dashboards URL (e.g., "http://localhost:5601"), or null if not started
     */
    public String getDashboardsUrl() {
        return SharedOpenSearchContainer.getDashboardsUrl();
    }
    
    /**
     * Gets the URL for accessing OpenSearch.
     * 
     * @return the OpenSearch URL (e.g., "http://localhost:9200"), or null if not started
     */
    public String getOpenSearchUrl() {
        return SharedOpenSearchContainer.getOpenSearchUrl();
    }
}
