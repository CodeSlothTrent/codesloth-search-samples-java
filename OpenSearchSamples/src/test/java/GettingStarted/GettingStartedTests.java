package GettingStarted;

import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.io.IOException;
import java.nio.file.Paths;

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
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GettingStartedTests {
    private static OpenSearchClient openSearchClient;
    private static final Logger logger = LogManager.getLogger(GettingStartedTests.class);

    @BeforeAll
    public static void setupBeforeAll() throws Exception {
        // Run docker-compose up command before tests start
        ProcessBuilder processBuilder = new ProcessBuilder("docker-compose", "-f", "infrastructure/docker-compose.yml", "up", "-d");
        Process process = processBuilder.start();
        var result = process.waitFor(); // Wait for the command to finish
        if (result != 0)
        {
            throw new Exception("Failed to start docker compose");
        }

        openSearchClient = new OpenSearchClient(ApacheHttpClient5TransportBuilder.builder(new HttpHost("http", "localhost", 9200)).build());
    }

    @AfterAll
    public static void tearDownAfterAll() throws Exception {
        // Tear down our infrastructure and associated volumes so we avoid persistent storage
        ProcessBuilder processBuilder = new ProcessBuilder("docker-compose", "-f", "infrastructure/docker-compose.yml", "down", "--volumes");
        Process process = processBuilder.start();
        var result = process.waitFor(); // Wait for the command to finish
        if (result != 0)
        {
            throw new Exception("Failed to start docker compose");
        }
    }

    /**
     * A simple request to start exploring the OpenSearch client is to list the indices in the cluster
     * When running the included docker-compose file, this will list the .kibana index
     */
    @Test
    public void ListIndices() throws IOException {
        var indices = openSearchClient.cat().indices();

        assertThat(indices.valueBody().size()).isEqualTo(1);
        assertThat(indices.valueBody().getFirst().index()).isEqualTo(".kibana_1");
    }
}
