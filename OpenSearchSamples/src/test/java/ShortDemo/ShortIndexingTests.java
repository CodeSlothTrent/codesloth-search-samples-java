package ShortDemo;

import ShortDemo.Documents.ProductDocument;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.GetResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for short field indexing in OpenSearch.
 * These tests demonstrate how short fields are indexed and stored in OpenSearch.
 *
 * Short fields store 16-bit signed integer values with a range from -32,768 to 32,767.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class ShortIndexingTests {
    private static final Logger logger = LogManager.getLogger(ShortIndexingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public ShortIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that short fields are indexed and stored correctly.
     * Short fields in OpenSearch can store values from -32,768 to 32,767.
     *
     * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
     *
     * @param stockValue  The stock value to index
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "0, 'Zero value is indexed correctly'",
            "1, 'Positive single digit value is indexed correctly'",
            "32767, 'Maximum short value (32767) is indexed correctly'",
            "-32768, 'Minimum short value (-32768) is indexed correctly'",
            "-1, 'Negative value is indexed correctly'",
            "1000, 'Mid-range positive value is indexed correctly'"
    })
    public void shortMapping_IndexesNumericValueCorrectly(short stockValue, String explanation) throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index a product document
            ProductDocument productDocument = new ProductDocument(1, "Mouse", stockValue);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Retrieve the document
            GetResponse<ProductDocument> result = openSearchClient.get(g -> g
                    .index(testIndex.getName())
                    .id(productDocument.getId()),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getStock()).as(explanation).isEqualTo(stockValue);
            assertThat(result.source().getName()).isEqualTo("Mouse");
        }
    }

    /**
     * This test verifies that short fields maintain precision and do not overflow
     * when storing values at the boundaries of the short range.
     *
     * @param stockValue  The stock value to test
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "32767, 'Maximum short value boundary test'",
            "-32768, 'Minimum short value boundary test'"
    })
    public void shortMapping_PreservesBoundaryValues(short stockValue, String explanation) throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index multiple documents to ensure consistent behavior
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Product A", stockValue),
                    new ProductDocument(2, "Product B", stockValue),
                    new ProductDocument(3, "Product C", stockValue)
            };
            testIndex.indexDocuments(productDocuments);

            // Verify each document
            for (ProductDocument doc : productDocuments) {
                GetResponse<ProductDocument> result = openSearchClient.get(g -> g
                        .index(testIndex.getName())
                        .id(doc.getId()),
                        ProductDocument.class
                );

                assertThat(result.found()).isTrue();
                assertThat(result.source().getStock())
                        .as(explanation + " for document " + doc.getId())
                        .isEqualTo(stockValue);
            }
        }
    }
}

