package FlattenedDemo;

import FlattenedDemo.Documents.ProductAttribute;
import FlattenedDemo.Documents.ProductWithFlattenedAttribute;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
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
     * Properties can be accessed using dotted notation: attribute.color, attribute.size, etc.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_IndexesObjectWithSubProperties() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create attribute as a strongly-typed record
            // ProductAttribute: (color, size)
            ProductAttribute attribute = new ProductAttribute("red", "large");

            // Create and index a product document
            ProductWithFlattenedAttribute productDocument = new ProductWithFlattenedAttribute(
                    "1", "Product1", attribute);
            testIndex.indexDocuments(new ProductWithFlattenedAttribute[]{productDocument});

            // Retrieve the document
            GetResponse<ProductWithFlattenedAttribute> result = openSearchClient.get(g -> g
                    .index(testIndex.getName())
                    .id(productDocument.getId()),
                    ProductWithFlattenedAttribute.class
            );

            // Verify the results
            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getName()).isEqualTo("Product1");
            
            // Verify attribute was stored (it will be deserialized as a ProductAttribute record)
            ProductAttribute storedAttribute = result.source().getAttribute();
            assertThat(storedAttribute).isNotNull();
            assertThat(storedAttribute.color()).isEqualTo("red");
            assertThat(storedAttribute.size()).isEqualTo("large");
        }
    }

}



