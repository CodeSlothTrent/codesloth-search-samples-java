package TestExtensions;

import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.Arrays;

/**
 * Singleton class that manages shared OpenSearch and OpenSearch Dashboards containers across all test classes.
 * <p>
 * These containers are started once before any tests run and stopped after all tests complete,
 * significantly improving test performance by avoiding the overhead of starting/stopping
 * containers for each test class.
 * </p>
 * <p>
 * Each test class should create its own unique index to avoid conflicts.
 * </p>
 * <p>
 * Fixed ports are used (mapped to default ports on the host):
 * <ul>
 *     <li>OpenSearch: 9200 (HTTP) - always available at http://localhost:9200</li>
 *     <li>OpenSearch Dashboards: 5601 (HTTP) - always available at http://localhost:5601</li>
 * </ul>
 * </p>
 */
public class SharedOpenSearchContainer {
    
    private static final Logger logger = LogManager.getLogger(SharedOpenSearchContainer.class);
    private static OpensearchContainer opensearchContainer;
    private static GenericContainer<?> dashboardsContainer;
    private static Network network;
    private static OpenSearchClient client;
    private static boolean started = false;
    
    // Standardized ports
    private static final int OPENSEARCH_HTTP_PORT = 9200;
    private static final int DASHBOARDS_HTTP_PORT = 5601;
    
    // Container names for network communication
    private static final String OPENSEARCH_CONTAINER_NAME = "opensearch-test";
    private static final String DASHBOARDS_CONTAINER_NAME = "opensearch-dashboards-test";
    
    /**
     * Starts the shared OpenSearch container if not already started.
     * Thread-safe singleton initialization.
     *
     * @return the OpenSearch client connected to the shared container
     * @throws Exception if container startup fails
     */
    public static synchronized OpenSearchClient getClient() throws Exception {
        if (!started) {
            start();
        }
        return client;
    }
    
    /**
     * Starts the shared container and creates a client.
     */
    private static void start() throws Exception {
        logger.info("Starting shared OpenSearch container (singleton)");
        
        // Check Docker daemon connectivity
        logger.info("Checking Docker daemon connectivity...");
        try {
            ProcessBuilder dockerCheck = new ProcessBuilder("docker", "info");
            Process dockerProcess = dockerCheck.start();
            boolean dockerFinished = dockerProcess.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
            
            if (!dockerFinished) {
                logger.error("Docker daemon check timed out after 10 seconds");
                dockerProcess.destroyForcibly();
                throw new DockerDaemonException(
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
                throw new DockerDaemonException(
                    "Docker daemon is not accessible (exit code: " + exitCode + ").\n" +
                    "Error: " + errorOutput.toString() + "\n" +
                    "Please ensure Docker Desktop is running before executing tests.\n" +
                    "On Windows, start Docker Desktop and wait for it to fully initialize.\n" +
                    "You can verify Docker is running by executing: docker ps"
                );
            }
            
            logger.info("Docker daemon is accessible and responding");
        } catch (DockerDaemonException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to check Docker daemon connectivity", e);
            throw new DockerDaemonException(
                "Unable to check Docker daemon status: " + e.getMessage() + "\n" +
                "Please ensure Docker Desktop is running before executing tests.\n" +
                "On Windows, start Docker Desktop and wait for it to fully initialize.\n" +
                "You can verify Docker is running by executing: docker ps",
                e
            );
        }
        
        // Create Docker network for container communication
        logger.info("Creating Docker network for OpenSearch and Dashboards...");
        network = Network.newNetwork();
        
        // Create and configure OpenSearch container with fixed port
        logger.info("Creating shared OpenSearch container with fixed port " + OPENSEARCH_HTTP_PORT + "...");
        opensearchContainer = new OpensearchContainer("opensearchproject/opensearch:2.11.1");
        opensearchContainer.withEnv("discovery.type", "single-node");
        opensearchContainer.withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m");
        opensearchContainer.withStartupTimeout(Duration.ofMinutes(2));
        opensearchContainer.withNetwork(network);
        opensearchContainer.withNetworkAliases(OPENSEARCH_CONTAINER_NAME);
        // Bind container port 9200 to host port 9200 (default OpenSearch port)
        opensearchContainer.setPortBindings(Arrays.asList(OPENSEARCH_HTTP_PORT + ":" + OPENSEARCH_HTTP_PORT));
        
        logger.info("Starting shared OpenSearch container...");
        opensearchContainer.start();
        
        // Port is now fixed, so mapped port should equal the fixed port
        int mappedPort = opensearchContainer.getMappedPort(OPENSEARCH_HTTP_PORT);
        logger.info("Shared OpenSearch container started successfully. Internal port: " + OPENSEARCH_HTTP_PORT + ", Mapped port: " + mappedPort);
        
        // Create and configure OpenSearch Dashboards container with fixed port
        logger.info("Creating OpenSearch Dashboards container with fixed port " + DASHBOARDS_HTTP_PORT + "...");
        dashboardsContainer = new GenericContainer<>("opensearchproject/opensearch-dashboards:2.11.1");
        dashboardsContainer.withNetwork(network);
        dashboardsContainer.withNetworkAliases(DASHBOARDS_CONTAINER_NAME);
        dashboardsContainer.withExposedPorts(DASHBOARDS_HTTP_PORT);
        // Bind container port 5601 to host port 5601 (default OpenSearch Dashboards port)
        dashboardsContainer.setPortBindings(Arrays.asList(DASHBOARDS_HTTP_PORT + ":" + DASHBOARDS_HTTP_PORT));
        dashboardsContainer.withEnv("OPENSEARCH_HOSTS", "http://" + OPENSEARCH_CONTAINER_NAME + ":" + OPENSEARCH_HTTP_PORT);
        dashboardsContainer.withEnv("DISABLE_SECURITY_DASHBOARDS_PLUGIN", "true");
        dashboardsContainer.waitingFor(Wait.forHttp("/").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
        
        logger.info("Starting OpenSearch Dashboards container...");
        dashboardsContainer.start();
        int dashboardsMappedPort = dashboardsContainer.getMappedPort(DASHBOARDS_HTTP_PORT);
        logger.info("OpenSearch Dashboards container started successfully. Internal port: " + DASHBOARDS_HTTP_PORT + ", Mapped port: " + dashboardsMappedPort);
        
        // Create client using fixed port (now matches internal port)
        String host = opensearchContainer.getHost();
        int clientPort = opensearchContainer.getMappedPort(OPENSEARCH_HTTP_PORT);
        logger.info("OpenSearch host: " + host + ", mapped port: " + clientPort);
        
        // Configure HttpClient with 5 minute socket timeout
        client = new OpenSearchClient(
            ApacheHttpClient5TransportBuilder.builder(new HttpHost("http", host, clientPort))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    RequestConfig config = RequestConfig.custom()
                        .setResponseTimeout(Timeout.ofSeconds(300))
                        .setConnectionRequestTimeout(Timeout.ofSeconds(300))
                        .setConnectTimeout(Timeout.ofSeconds(300))
                        .build();
                    return httpClientBuilder.setDefaultRequestConfig(config);
                })
                .setMapper(new org.opensearch.client.json.jackson.JacksonJsonpMapper())
                .build()
        );
        
        started = true;
        
        logger.info("Shared OpenSearch and Dashboards containers setup completed successfully");
        logger.info("OpenSearch available at: http://localhost:" + clientPort);
        logger.info("OpenSearch Dashboards available at: http://localhost:" + dashboardsMappedPort);
    }
    
    /**
     * Gets the OpenSearch container instance (for advanced use cases).
     * 
     * @return the OpenSearch container instance, or null if not started
     */
    public static OpensearchContainer getContainer() {
        return opensearchContainer;
    }
    
    /**
     * Gets the OpenSearch Dashboards container instance (for advanced use cases).
     * 
     * @return the Dashboards container instance, or null if not started
     */
    public static GenericContainer<?> getDashboardsContainer() {
        return dashboardsContainer;
    }
    
    /**
     * Gets the URL for accessing OpenSearch Dashboards.
     * 
     * @return the Dashboards URL (e.g., "http://localhost:5601"), or null if not started
     */
    public static String getDashboardsUrl() {
        if (!started || dashboardsContainer == null) {
            return null;
        }
        int mappedPort = dashboardsContainer.getMappedPort(DASHBOARDS_HTTP_PORT);
        return "http://localhost:" + mappedPort;
    }
    
    /**
     * Gets the URL for accessing OpenSearch.
     * 
     * @return the OpenSearch URL (e.g., "http://localhost:9200"), or null if not started
     */
    public static String getOpenSearchUrl() {
        if (!started || opensearchContainer == null) {
            return null;
        }
        int mappedPort = opensearchContainer.getMappedPort(OPENSEARCH_HTTP_PORT);
        return "http://localhost:" + mappedPort;
    }
    
    /**
     * Checks if the containers are started.
     * 
     * @return true if the containers are running
     */
    public static boolean isStarted() {
        return started;
    }
    
}

