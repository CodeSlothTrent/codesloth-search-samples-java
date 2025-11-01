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
import org.opensearch.client.opensearch.core.TermvectorsResponse;
import org.opensearch.client.opensearch.core.termvectors.TermVector;

import java.util.Map;

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

    /**
     * This test verifies that flattened field sub-properties are indexed as keywords
     * by observing the terms produced using a term vector query.
     * Each sub-property (like attribute.color and attribute.size) produces a single keyword token.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_SubPropertiesIndexedAsKeywords() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create attribute with specific values that include capital letters and punctuation
            // This demonstrates how flattened fields tokenize and normalize keyword values
            ProductAttribute attribute = new ProductAttribute("Red-Metal!", "Extra-Large");

            // Create and index a product document
            ProductWithFlattenedAttribute productDocument = new ProductWithFlattenedAttribute(
                    "1", "Product1", attribute);
            testIndex.indexDocuments(new ProductWithFlattenedAttribute[]{productDocument});

            // This demonstrates that no tokens are created at this level
            TermvectorsResponse topLevelResults = openSearchClient.termvectors(t -> t
                    .index(testIndex.getName())
                    .id(productDocument.getId())
                    .fields("attribute")
            );

            // Verify that no tokens are produced at the top-level flattened field
            // Flattened fields don't create tokens at the parent level, only at sub-property levels
            assertThat(topLevelResults.found()).isTrue();
            Map<String, TermVector> topLevelTermVectors = topLevelResults.termVectors();
            // The term vectors map should be empty or contain no terms because flattened fields
            // only index sub-properties, not the parent field itself
            assertThat(topLevelTermVectors).isEmpty();

            // Get term vectors for the color sub-property
            TermvectorsResponse colorResult = openSearchClient.termvectors(t -> t
                    .index(testIndex.getName())
                    .id(productDocument.getId())
                    .fields("attribute.color")
            );

            // Verify color term vectors - flattened fields index sub-properties as keywords
            assertThat(colorResult.found()).isTrue();
            Map<String, TermVector> colorTermVectors = colorResult.termVectors();
            assertThat(colorTermVectors).hasSize(1);
            TermVector colorTermVector = colorTermVectors.get("attribute.color");
            assertThat(colorTermVector).isNotNull();
            
            // Assert that the value is tokenized and normalized - "Red-Metal!" becomes "red" and "metal"
            var colorTerms = colorTermVector.terms();
            assertThat(colorTerms).hasSize(2);
            assertThat(colorTerms).containsKey("red");
            assertThat(colorTerms).containsKey("metal");
            assertThat(colorTerms.get("red").termFreq()).isEqualTo(1);
            assertThat(colorTerms.get("metal").termFreq()).isEqualTo(1);

            // Get term vectors for the size sub-property
            TermvectorsResponse sizeResult = openSearchClient.termvectors(t -> t
                    .index(testIndex.getName())
                    .id(productDocument.getId())
                    .fields("attribute.size")
            );

            // Verify size term vectors - flattened fields index sub-properties as keywords
            assertThat(sizeResult.found()).isTrue();
            Map<String, TermVector> sizeTermVectors = sizeResult.termVectors();
            assertThat(sizeTermVectors).hasSize(1);
            TermVector sizeTermVector = sizeTermVectors.get("attribute.size");
            assertThat(sizeTermVector).isNotNull();
            
            // Assert that the value is tokenized and normalized - "Extra-Large" becomes "extra" and "large"
            var sizeTerms = sizeTermVector.terms();
            assertThat(sizeTerms).hasSize(2);
            assertThat(sizeTerms).containsKey("extra");
            assertThat(sizeTerms).containsKey("large");
            assertThat(sizeTerms.get("extra").termFreq()).isEqualTo(1);
            assertThat(sizeTerms.get("large").termFreq()).isEqualTo(1);
        }
    }

}




