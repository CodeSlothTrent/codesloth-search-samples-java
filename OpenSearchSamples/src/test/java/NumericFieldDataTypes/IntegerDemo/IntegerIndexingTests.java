package NumericFieldDataTypes.IntegerDemo;

import NumericFieldDataTypes.IntegerDemo.Documents.ProductDocument;
import TestExtensions.LoggingOpenSearchClient;
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
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.GetResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for integer field indexing in OpenSearch.
 * These tests demonstrate how integer fields are indexed and stored in OpenSearch.
 *
 * Integer fields store 32-bit signed integer values with a range from -2,147,483,648 to 2,147,483,647.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class IntegerIndexingTests {
    private static final Logger logger = LogManager.getLogger(IntegerIndexingTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public IntegerIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that integer fields are indexed and stored correctly.
     * Integer fields in OpenSearch can store values from -2,147,483,648 to 2,147,483,647.
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
            "2147483647, 'Maximum integer value (2147483647) is indexed correctly'",
            "-2147483648, 'Minimum integer value (-2147483648) is indexed correctly'",
            "-1, 'Negative value is indexed correctly'",
            "100000, 'Mid-range positive value is indexed correctly'"
    })
    public void integerMapping_IndexesNumericValueCorrectly(int stockValue, String explanation) throws Exception {
        // Create a test index with integer mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.integer(i -> i))))) {

            // Create and index a product document
            ProductDocument productDocument = new ProductDocument(1, "Mouse", stockValue);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Retrieve the document
            GetResponse<ProductDocument> result = loggingOpenSearchClient.getClient().get(g -> g
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
     * This test verifies that integer fields maintain precision and do not overflow
     * when storing values at the boundaries of the integer range.
     *
     * @param stockValue  The stock value to test
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "2147483647, 'Maximum integer value boundary test'",
            "-2147483648, 'Minimum integer value boundary test'"
    })
    public void integerMapping_PreservesBoundaryValues(int stockValue, String explanation) throws Exception {
        // Create a test index with integer mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.integer(i -> i))))) {

            // Create and index multiple documents to ensure consistent behavior
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Product A", stockValue),
                    new ProductDocument(2, "Product B", stockValue),
                    new ProductDocument(3, "Product C", stockValue)
            };
            testIndex.indexDocuments(productDocuments);

            // Verify each document
            for (ProductDocument doc : productDocuments) {
                GetResponse<ProductDocument> result = loggingOpenSearchClient.getClient().get(g -> g
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

