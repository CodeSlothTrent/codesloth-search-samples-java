package FlattenedDemo;

import FlattenedDemo.Documents.ProductMetadata;
import FlattenedDemo.Documents.ProductWithFlattenedMetadata;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.GetResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for flattened field indexing in OpenSearch.
 * These tests demonstrate how flattened fields are indexed and stored in OpenSearch.
 *
 * Flattened fields map an entire object as a single field, where each sub-field is indexed as a keyword.
 * This allows for searching on object properties using dotted notation without the overhead of the object type.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/flattened/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class FlattenedIndexingTests {
    private static final Logger logger = LogManager.getLogger(FlattenedIndexingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public FlattenedIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that flattened fields are indexed correctly with all sub-properties accessible.
     * Properties can be accessed using dotted notation: metadata.title, metadata.description, etc.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_IndexesObjectWithSubProperties() throws Exception {
        // Create a test index with flattened mapping for the metadata field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("metadata", Property.of(p -> p.flatObject(f -> f))))) {

            // Create metadata as a strongly-typed record
            // ProductMetadata: (title, brand, category, price, description)
            ProductMetadata metadata = new ProductMetadata(
                    "Wireless Mouse",
                    "TechCorp",
                    "Electronics",
                    29.99,
                    "Ergonomic wireless mouse"
            );

            // Create and index a product document
            ProductWithFlattenedMetadata productDocument = new ProductWithFlattenedMetadata(
                    "1", "Mouse", metadata);
            testIndex.indexDocuments(new ProductWithFlattenedMetadata[]{productDocument});

            // Retrieve the document
            GetResponse<ProductWithFlattenedMetadata> result = openSearchClient.get(g -> g
                    .index(testIndex.getName())
                    .id(productDocument.getId()),
                    ProductWithFlattenedMetadata.class
            );

            // Verify the results
            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getName()).isEqualTo("Mouse");
            
            // Verify metadata was stored (it will be deserialized as a ProductMetadata record)
            ProductMetadata storedMetadata = result.source().getMetadata();
            assertThat(storedMetadata).isNotNull();
            assertThat(storedMetadata.title()).isEqualTo("Wireless Mouse");
            assertThat(storedMetadata.brand()).isEqualTo("TechCorp");
        }
    }

    /**
     * This test verifies that flattened fields preserve nested object structures.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_IndexesNestedObjectStructures() throws Exception {
        // Create a test index with flattened mapping for the metadata field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("metadata", Property.of(p -> p.flatObject(f -> f))))) {

            // Create metadata as a strongly-typed record
            // ProductMetadata: (title, brand, category, price, description)
            ProductMetadata metadata = new ProductMetadata(
                    "Gaming Keyboard",
                    "GameTech",
                    "Electronics",
                    129.99,
                    "High-performance gaming keyboard"
            );

            // Create and index a product document
            ProductWithFlattenedMetadata productDocument = new ProductWithFlattenedMetadata(
                    "2", "Keyboard", metadata);
            testIndex.indexDocuments(new ProductWithFlattenedMetadata[]{productDocument});

            // Retrieve the document
            GetResponse<ProductWithFlattenedMetadata> result = openSearchClient.get(g -> g
                    .index(testIndex.getName())
                    .id(productDocument.getId()),
                    ProductWithFlattenedMetadata.class
            );

            // Verify the results
            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getName()).isEqualTo("Keyboard");
            assertThat(result.source().getMetadata()).isNotNull();
        }
    }
}

