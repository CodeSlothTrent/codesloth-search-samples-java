package TestExtensions;

import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.*;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.testcontainers.OpensearchContainer;

import java.time.Duration;

/**
 * JUnit 5 extension to manage OpenSearch resources using Testcontainers.
 * <p>
 * This extension provides the following functionalities:
 * <ul>
 *     <li>Starts an OpenSearch container using Testcontainers before all tests run.</li>
 *     <li>Creates and manages an {@link OpenSearchSharedResource} instance, which includes an {@link OpenSearchClient} connected to the OpenSearch server.</li>
 *     <li>Stops and removes the container after all tests complete to ensure a clean state.</li>
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
 *     <li>Uses Testcontainers for reliable Docker container management across all environments.</li>
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
    private OpensearchContainer opensearchContainer;
    private static final Logger logger = LogManager.getLogger(OpenSearchResourceManagementExtension.class);

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        logger.info("Starting beforeAll setup with Testcontainers");

        // Check if Docker is accessible before attempting to start container
        logger.info("Checking Docker daemon connectivity...");
        try {
            ProcessBuilder dockerCheck = new ProcessBuilder("docker", "info");
            Process dockerProcess = dockerCheck.start();
            boolean dockerFinished = dockerProcess.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);

            if (!dockerFinished) {
                logger.error("Docker daemon check timed out after 10 seconds");
                dockerProcess.destroyForcibly();
                throw new Exception(
                    "Docker daemon is not responding. Please ensure Docker Desktop is running before executing tests.\n" +
                    "On Windows, start Docker Desktop and wait for it to fully initialize.\n" +
                    "You can verify Docker is running by executing: docker ps"
                );
            }

            int exitCode = dockerProcess.exitValue();
            if (exitCode != 0) {
                java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(dockerProcess.getErrorStream()));
                String errorLine;
                StringBuilder errorOutput = new StringBuilder();
                while ((errorLine = reader.readLine()) != null) {
                    errorOutput.append(errorLine).append("\n");
                }

                logger.error("Docker daemon check failed with exit code: " + exitCode);
                logger.error("Docker error output: " + errorOutput.toString());
                throw new Exception(
                    "Docker daemon is not accessible (exit code: " + exitCode + ").\n" +
                    "Error: " + errorOutput.toString() + "\n" +
                    "Please ensure Docker Desktop is running before executing tests.\n" +
                    "On Windows, start Docker Desktop and wait for it to fully initialize.\n" +
                    "You can verify Docker is running by executing: docker ps"
                );
            }

            logger.info("Docker daemon is accessible and responding");
        } catch (Exception e) {
            if (e.getMessage().contains("Docker daemon")) {
                throw e; // Re-throw our custom exceptions
            }
            logger.error("Failed to check Docker daemon connectivity", e);
            throw new Exception(
                "Unable to check Docker daemon status: " + e.getMessage() + "\n" +
                "Please ensure Docker Desktop is running before executing tests.\n" +
                "On Windows, start Docker Desktop and wait for it to fully initialize.\n" +
                "You can verify Docker is running by executing: docker ps",
                e
            );
        }

        // Create and start OpenSearch container using Testcontainers
        // Using OpenSearch 2.0.1 to match the docker-compose configuration
        logger.info("Creating OpenSearch container...");
        opensearchContainer = new OpensearchContainer("opensearchproject/opensearch:2.0.1");
        opensearchContainer.withEnv("discovery.type", "single-node");
        opensearchContainer.withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m");
        opensearchContainer.withStartupTimeout(Duration.ofMinutes(1));

        logger.info("Starting OpenSearch container...");
        opensearchContainer.start();
        logger.info("OpenSearch container started successfully");

        // Get the HTTP host URL from the container
        String httpHostAddress = opensearchContainer.getHttpHostAddress();
        logger.info("OpenSearch HTTP host address: " + httpHostAddress);

        // Parse the host and port - httpHostAddress format can be "host:port", "//host:port", or "http://host:port"
        String cleanAddress = httpHostAddress.replaceFirst("^https?://", "").replaceFirst("^//", "");
        String[] parts = cleanAddress.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        logger.info("Parsed host: " + host + ", port: " + port);

        // Create OpenSearch client
        logger.info("Creating OpenSearch client");
        var openSearchClient = new OpenSearchClient(
            ApacheHttpClient5TransportBuilder.builder(new HttpHost("http", host, port))
                .setMapper(new org.opensearch.client.json.jackson.JacksonJsonpMapper())
                .build());

        openSearchSharedResource = new OpenSearchSharedResource(openSearchClient);
        logger.info("Setup completed successfully");
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        logger.info("Starting afterAll teardown");

        if (opensearchContainer != null) {
            logger.info("Stopping OpenSearch container");
            opensearchContainer.stop();
            logger.info("OpenSearch container stopped successfully");
        }

        logger.info("Teardown completed successfully");
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        // Only resolve a specific type, not generic Strings
        return parameterContext.getParameter().getType() == OpenSearchSharedResource.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return openSearchSharedResource;
    }
}
