package TestExtensions;

import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.opensearch.testcontainers.OpensearchContainer;

import java.time.Duration;

/**
 * Singleton class that manages a shared OpenSearch container across all test classes.
 * <p>
 * This container is started once before any tests run and stopped after all tests complete,
 * significantly improving test performance by avoiding the overhead of starting/stopping
 * containers for each test class.
 * </p>
 * <p>
 * Each test class should create its own unique index to avoid conflicts.
 * </p>
 */
public class SharedOpenSearchContainer {
    
    private static final Logger logger = LogManager.getLogger(SharedOpenSearchContainer.class);
    private static OpensearchContainer container;
    private static OpenSearchClient client;
    private static boolean started = false;
    
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
        
        // Create and start container
        logger.info("Creating shared OpenSearch container...");
        container = new OpensearchContainer("opensearchproject/opensearch:2.11.1");
        container.withEnv("discovery.type", "single-node");
        container.withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m");
        container.withStartupTimeout(Duration.ofMinutes(2));
        
        logger.info("Starting shared OpenSearch container...");
        container.start();
        logger.info("Shared OpenSearch container started successfully");
        
        // Create client
        String httpHostAddress = container.getHttpHostAddress();
        logger.info("OpenSearch HTTP host address: " + httpHostAddress);
        
        String cleanAddress = httpHostAddress.replaceFirst("^https?://", "").replaceFirst("^//", "");
        String[] parts = cleanAddress.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        logger.info("Parsed host: " + host + ", port: " + port);
        
        client = new OpenSearchClient(
            ApacheHttpClient5TransportBuilder.builder(new HttpHost("http", host, port))
                .setMapper(new org.opensearch.client.json.jackson.JacksonJsonpMapper())
                .build()
        );
        
        started = true;
        
        logger.info("Shared OpenSearch container setup completed successfully");
    }
    
    /**
     * Gets the container instance (for advanced use cases).
     * 
     * @return the container instance, or null if not started
     */
    public static OpensearchContainer getContainer() {
        return container;
    }
    
    /**
     * Checks if the container is started.
     * 
     * @return true if the container is running
     */
    public static boolean isStarted() {
        return started;
    }
    
}

