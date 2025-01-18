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
}
