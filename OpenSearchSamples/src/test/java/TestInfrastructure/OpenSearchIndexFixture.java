package TestInfrastructure;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.indices.IndexSettings;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * A fixture for creating and managing OpenSearch test indices.
 * This class provides methods to create test indices with specific mappings and settings.
 */
public class OpenSearchIndexFixture {
    private final OpenSearchClient openSearchClient;
    private final OpenSearchRequestLogger logger;

    /**
     * Creates a new OpenSearchIndexFixture with the provided OpenSearchClient and logger.
     *
     * @param openSearchClient The OpenSearch client to use for operations
     * @param logger The logger instance for this test class (can be null if logging is not needed)
     */
    public OpenSearchIndexFixture(OpenSearchClient openSearchClient, OpenSearchRequestLogger logger) {
        this.openSearchClient = openSearchClient;
        this.logger = logger;
    }

    /**
     * Gets the OpenSearch client.
     *
     * @return The OpenSearch client
     */
    public OpenSearchClient getOpenSearchClient() {
        return openSearchClient;
    }

    /**
     * Creates a test index with the specified mapping.
     *
     * @param mappingConsumer Consumer to configure the mapping
     * @return A new OpenSearchTestIndex instance
     * @throws IOException If an I/O error occurs during index creation
     */
    public OpenSearchTestIndex createTestIndex(Consumer<TypeMapping.Builder> mappingConsumer) throws IOException {
        return createTestIndex(mappingConsumer, null);
    }

    /**
     * Creates a test index with the specified mapping and settings.
     *
     * @param mappingConsumer Consumer to configure the mapping
     * @param settingsConsumer Consumer to configure the settings (optional, can be null)
     * @return A new OpenSearchTestIndex instance
     * @throws IOException If an I/O error occurs during index creation
     */
    public OpenSearchTestIndex createTestIndex(Consumer<TypeMapping.Builder> mappingConsumer, 
                                              Consumer<IndexSettings.Builder> settingsConsumer) throws IOException {
        OpenSearchTestIndex testIndex = new OpenSearchTestIndex(openSearchClient, logger);
        testIndex.createIndex(mappingConsumer, settingsConsumer);
        return testIndex;
    }
} 