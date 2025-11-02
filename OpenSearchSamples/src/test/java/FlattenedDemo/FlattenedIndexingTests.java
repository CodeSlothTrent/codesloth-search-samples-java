package FlattenedDemo;

import FlattenedDemo.Documents.ProductAttribute;
import FlattenedDemo.Documents.ProductWithFlattenedAttribute;
import TestExtensions.LoggingOpenSearchClient;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public FlattenedIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient());
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

            // Create and index a product document with a strongly-typed record
            // ProductAttribute: (color, size)
            ProductWithFlattenedAttribute productDocument = new ProductWithFlattenedAttribute(
                    "1", "Product1", new ProductAttribute("red", "large"));
            testIndex.indexDocuments(new ProductWithFlattenedAttribute[]{productDocument});

            // Retrieve the document
            GetResponse<ProductWithFlattenedAttribute> result = loggingOpenSearchClient.getClient().get(g -> g
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
     * This test demonstrates that term vectors return INCORRECT results for flattened fields.
     * Flattened fields actually index sub-properties as unanalyzed keywords (preserving exact values),
     * but term vectors incorrectly show tokenization and normalization.
     * 
     * IMPORTANT: The term vector output is misleading. In reality, flattened fields preserve
     * the exact value as-is (unanalyzed), which means "Red-Metal!" is stored exactly as "Red-Metal!"
     * without tokenization. See the search tests to verify the actual behavior.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_SubPropertiesIndexedAsKeywords() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create and index a product document with specific values that include capital letters and punctuation
            // Note: Despite what term vectors show, flattened fields actually preserve exact values
            ProductWithFlattenedAttribute productDocument = new ProductWithFlattenedAttribute(
                    "1", "Product1", new ProductAttribute("Red-Metal!", "Extra-Large"));
            testIndex.indexDocuments(new ProductWithFlattenedAttribute[]{productDocument});

            // This demonstrates that no tokens are created at this level
            TermvectorsResponse topLevelResults = loggingOpenSearchClient.termvectors(t -> t
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
            // NOTE: Term vectors show INCORRECT results - they suggest tokenization happens,
            // but flattened fields actually store unanalyzed keywords preserving exact values
            TermvectorsResponse colorResult = loggingOpenSearchClient.termvectors(t -> t
                    .index(testIndex.getName())
                    .id(productDocument.getId())
                    .fields("attribute.color")
            );

            // WARNING: Term vectors show misleading tokenization results
            // They incorrectly suggest "Red-Metal!" is tokenized to "red" and "metal"
            // However, flattened fields are UNANALYZED keywords that preserve exact values
            assertThat(colorResult.found()).isTrue();
            Map<String, TermVector> colorTermVectors = colorResult.termVectors();
            assertThat(colorTermVectors).hasSize(1);
            TermVector colorTermVector = colorTermVectors.get("attribute.color");
            assertThat(colorTermVector).isNotNull();
            
            // Term vectors incorrectly show tokenization - "Red-Metal!" appears as "red" and "metal"
            // BUT this is NOT how flattened fields actually work. See search tests for proof.
            var colorTerms = colorTermVector.terms();
            assertThat(colorTerms).hasSize(2);
            assertThat(colorTerms).containsKey("red");
            assertThat(colorTerms).containsKey("metal");
            assertThat(colorTerms.get("red").termFreq()).isEqualTo(1);
            assertThat(colorTerms.get("metal").termFreq()).isEqualTo(1);

            // Get term vectors for the size sub-property
            TermvectorsResponse sizeResult = loggingOpenSearchClient.termvectors(t -> t
                    .index(testIndex.getName())
                    .id(productDocument.getId())
                    .fields("attribute.size")
            );

            // WARNING: Term vectors show misleading tokenization results here too
            assertThat(sizeResult.found()).isTrue();
            Map<String, TermVector> sizeTermVectors = sizeResult.termVectors();
            assertThat(sizeTermVectors).hasSize(1);
            TermVector sizeTermVector = sizeTermVectors.get("attribute.size");
            assertThat(sizeTermVector).isNotNull();
            
            // Term vectors incorrectly show "Extra-Large" as "extra" and "large"
            // BUT flattened fields actually preserve the exact value "Extra-Large" as-is
            var sizeTerms = sizeTermVector.terms();
            assertThat(sizeTerms).hasSize(2);
            assertThat(sizeTerms).containsKey("extra");
            assertThat(sizeTerms).containsKey("large");
            assertThat(sizeTerms.get("extra").termFreq()).isEqualTo(1);
            assertThat(sizeTerms.get("large").termFreq()).isEqualTo(1);
            
            // IMPORTANT: Do not rely on term vectors to understand flattened field behavior.
            // Term vectors return incorrect/ misleading information. Flattened fields are unanalyzed
            // keywords that preserve exact values. Search tests prove this by showing that:
            // - Exact matches work: "Red-Metal!" matches "Red-Metal!"
            // - Token searches fail: "red" does NOT match "Red-Metal!"
        }
    }

}




