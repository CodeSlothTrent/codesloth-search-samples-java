package TestInfrastructure;

import KeywordDemo.Documents.IDocumentWithId;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexResponse;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Represents a test index in OpenSearch that can be created, used, and disposed of during tests.
 * This class implements AutoCloseable to ensure proper cleanup of resources.
 */
public class OpenSearchTestIndex implements AutoCloseable {
    private final OpenSearchClient openSearchClient;
    private final String name;

    /**
     * Creates a new test index with a random UUID name.
     *
     * @param openSearchClient The OpenSearch client to use for operations
     */
    public OpenSearchTestIndex(OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
        this.name = UUID.randomUUID().toString();
    }

    /**
     * Gets the name of the test index.
     *
     * @return The index name
     */
    public String getName() {
        return name;
    }

    /**
     * Creates the index with the specified mapping and settings.
     *
     * @param mappingConsumer  Consumer to configure the mapping
     * @param settingsConsumer Consumer to configure the settings (optional)
     * @throws IOException If an I/O error occurs during the operation
     */
    public void createIndex(Consumer<TypeMapping.Builder> mappingConsumer, Consumer<IndexSettings.Builder> settingsConsumer) throws IOException {
        TypeMapping.Builder mappingBuilder = new TypeMapping.Builder();
        mappingConsumer.accept(mappingBuilder);

        CreateIndexRequest.Builder requestBuilder = new CreateIndexRequest.Builder()
                .index(name)
                .mappings(mappingBuilder.build());

        if (settingsConsumer != null) {
            IndexSettings.Builder settingsBuilder = new IndexSettings.Builder();
            settingsConsumer.accept(settingsBuilder);
            requestBuilder.settings(settingsBuilder.build());
        }

        CreateIndexResponse response = openSearchClient.indices().create(requestBuilder.build());

        if (!response.acknowledged()) {
            throw new IOException("Failed to create index: " + name);
        }
    }

    /**
     * Indexes the provided documents into the test index.
     *
     * @param documents       Array of documents to index
     * @param IDocumentWithId The document type
     * @throws IOException If an I/O error occurs during the operation
     */
    public void indexDocuments(IDocumentWithId[] documents) throws IOException {
        // Use the bulk API to index multiple documents
        var bulkRequest = new BulkRequest.Builder();

        Arrays.stream(documents).forEach(doc -> {
            bulkRequest.operations(op -> op
                    .index(idx -> idx
                            .id(doc.getId())
                            .index(name)
                            .document(doc)
                    )
            );
        });

        var bulkResponse = openSearchClient.bulk(bulkRequest.refresh(Refresh.True).build());

        if (bulkResponse.errors()) {
            throw new IOException("Failed to index documents: " + bulkResponse.toString());
        }
    }

    /**
     * Closes the test index by deleting it from OpenSearch.
     *
     * @throws Exception If an error occurs during deletion
     */
    @Override
    public void close() throws Exception {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest.Builder()
                    .index(name)
                    .build();
            openSearchClient.indices().delete(request);
        } catch (Exception e) {
            // Log the exception but don't rethrow - we tried our best to clean up
            System.err.println("Error deleting test index: " + e.getMessage());
        }
    }
} 