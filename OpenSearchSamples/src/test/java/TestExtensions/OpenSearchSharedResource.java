package TestExtensions;

import org.opensearch.client.opensearch.OpenSearchClient;

public class OpenSearchSharedResource {
    private OpenSearchClient openSearchClient;

    public OpenSearchSharedResource(OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    public OpenSearchClient getOpenSearchClient() {
        return openSearchClient;
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
