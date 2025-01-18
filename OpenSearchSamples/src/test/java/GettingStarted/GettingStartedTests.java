package GettingStarted;

import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.HealthStatus;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.GetIndexRequest;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The getting started tests contain basic information for getting up and running with these code samples
 * <p>
 * This project's pom.xml has been configured with the following dependencies in order run:
 * Unit testing dependencies:
 * <ul>
 *      <li>org.junit.jupiter/junit-jupiter</li>
 *       <li>org.mockito/mockito-inline</li>
 *       <li>org.mockito/mockito-junit-jupiter, org.assertj/assertj-core</li>
 * <p>
 * <p>
 * Logging dependencies:
 * <ul>
 *      <li>org.apache.logging.log4j/log4j-api</li>
 *      <li>org.apache.logging.log4j/log4j-core</li>
 *      <li>org.apache.logging.log4j/log4j-slf4j-impl</li>
 *      </ul>
 * OpenSearch dependencies:
 * <ul>
 *      <li>org.opensearch.client/opensearch-java</li>
 *      <li>org.apache.httpcomponents.client5/httpclient5</li>
 *      <li>org.apache.httpcomponents.core5/httpcore5</li>
 * </ul>
 * It is recommended to share OpenSearch clients, rather than creating many, so @BeforeAll setup creates this shared client.
 * Specifically for this file we want tests to run sequentially
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class GettingStartedTests {
    private static final Logger logger = LogManager.getLogger(GettingStartedTests.class);

    private OpenSearchClient openSearchClient;

    public GettingStartedTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    /**
     * A simple request to start exploring the OpenSearch client is to get the health of the cluster
     * The setup code will ensure that the cluster is ready to be queried, as issuing a request while it is starting up
     * will result in an exception org.apache.hc.core5.http.ConnectionClosedException: Connection closed by peer
     */
    @Test
    @Order(1)
    public void GetClusterHealth_HasGreenStatus() throws IOException {
        var healthResponse = openSearchClient.cluster().health();
        assertThat(healthResponse.status()).isEqualTo(HealthStatus.Green);
    }

    @Test
    public void GetClusterHealth_HasOneDataNode() throws IOException {
        var indices = openSearchClient.cluster().health();
        assertThat(indices.numberOfDataNodes()).isEqualTo(1);
    }

    @Test
    public void OpenSearchClient_CanCreate_AndDelete_AnIndex() throws IOException {
        var testIndexName = "test-index";
        var createIndexRequest = new CreateIndexRequest
                .Builder()
                .index(testIndexName)
                .build();

        // Create the index
        var createIndexResponse = openSearchClient.indices().create(createIndexRequest);

        // Assert that it was successful
        assertThat(createIndexResponse.acknowledged()).isTrue();
        assertThat(createIndexResponse.index()).isEqualTo(testIndexName);

        // Look up the created index
        var getIndexRequest = new GetIndexRequest
                .Builder()
                .index(testIndexName)
                .build();

        // Make a call to fetch the index info by index name
        var getIndexResponse = openSearchClient.indices().get(getIndexRequest);
        // Assert that it was successful
        assertThat(getIndexResponse.result().size()).isEqualTo(1);
        assertThat(getIndexResponse.result()).containsKey(testIndexName);

        var deleteIndexRequest = new DeleteIndexRequest
                .Builder()
                .index(testIndexName)
                .build();

        var deleteIndexResponse = openSearchClient.indices().delete(deleteIndexRequest);
        assertThat(deleteIndexResponse.acknowledged()).isTrue();
    }
}
