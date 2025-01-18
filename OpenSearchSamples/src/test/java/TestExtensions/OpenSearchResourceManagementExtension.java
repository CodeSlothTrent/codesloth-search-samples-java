package TestExtensions;

import GettingStarted.GettingStartedTests;
import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.*;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.HealthStatus;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.time.Duration;

/**
 * JUnit 5 extension to manage OpenSearch resources and infrastructure setup/teardown during tests.
 * <p>
 * This extension provides the following functionalities:
 * <ul>
 *     <li>Starts a Dockerized OpenSearch instance using a docker-compose file before all tests run.</li>
 *     <li>Creates and manages an {@link OpenSearchSharedResource} instance, which includes an {@link OpenSearchClient} connected to the OpenSearch server.</li>
 *     <li>Tears down the Dockerized infrastructure, including volumes, after all tests complete to ensure a clean state.</li>
 *     <li>Resolves parameters annotated in test classes, injecting shared resources like the {@link OpenSearchSharedResource} instance into test constructors or methods.</li>
 * </ul>
 * <p>
 * This extension is particularly useful for integration tests requiring an OpenSearch instance.
 *
 * <h2>Key Features:</h2>
 * <ul>
 *     <li>Implements {@link BeforeAllCallback} to set up infrastructure before tests.</li>
 *     <li>Implements {@link AfterAllCallback} to clean up resources after tests.</li>
 *     <li>Implements {@link ParameterResolver} to inject shared test dependencies.</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * @ExtendWith(OpenSearchResourceManagementExtension.class)
 * public class MyTests {
 *     private final OpenSearchSharedResource sharedResource;
 *
 *     public MyTests(OpenSearchSharedResource sharedResource) {
 *         this.sharedResource = sharedResource;
 *     }
 *
 *     @Test
 *     void test1() {
 *         Assertions.assertNotNull(sharedResource.getOpenSearchClient());
 *     }
 *
 *     @Test
 *     void test2() {
 *         // Use sharedResource for further testing
 *     }
 * }
 * }</pre>
 *
 * @see OpenSearchClient
 * @see OpenSearchSharedResource
 * @see BeforeAllCallback
 * @see AfterAllCallback
 * @see ParameterResolver
 */
public class OpenSearchResourceManagementExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private OpenSearchSharedResource openSearchSharedResource;
    private static final Logger logger = LogManager.getLogger(OpenSearchResourceManagementExtension.class);

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        // Run docker-compose up command before tests start
        ProcessBuilder processBuilder = new ProcessBuilder("docker-compose", "-f", "infrastructure/docker-compose.yml", "up", "-d");
        Process process = processBuilder.start();
        var result = process.waitFor(); // Wait for the command to finish
        if (result != 0) {
            throw new Exception("Failed to start docker compose");
        }

        var openSearchClient = new OpenSearchClient(ApacheHttpClient5TransportBuilder.builder(new HttpHost("http", "localhost", 9200)).build());

        var clusterIsReady = false;
        while (!clusterIsReady) {
            logger.info("Waiting for cluster to start");
            Thread.sleep(Duration.ofSeconds(1));
            try {
                var status = openSearchClient.cluster().health().status();
                clusterIsReady = status == HealthStatus.Green;
            }
            catch (Exception e)
            {
                // Uncomment for debugging
                // logger.info("Exception", e);
            }
        }

        openSearchSharedResource = new OpenSearchSharedResource(openSearchClient);
        logger.info("Setup completed");
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        logger.info("Tearing down");
        // Tear down our infrastructure and associated volumes so we avoid persistent storage
        ProcessBuilder processBuilder = new ProcessBuilder("docker-compose", "-f", "infrastructure/docker-compose.yml", "down", "--volumes");
        Process process = processBuilder.start();
        var result = process.waitFor(); // Wait for the command to finish
        if (result != 0) {
            throw new Exception("Failed to start docker compose");
        }
        logger.info("Finished tearing down");
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return true;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return openSearchSharedResource;
    }
}