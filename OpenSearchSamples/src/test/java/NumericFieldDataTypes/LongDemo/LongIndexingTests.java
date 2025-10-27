package NumericFieldDataTypes.LongDemo;

import NumericFieldDataTypes.LongDemo.Documents.ProductDocument;
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
 * Tests for long field indexing in OpenSearch.
 * These tests demonstrate how long fields are indexed and stored in OpenSearch.
 *
 * Long fields store 64-bit signed integer values with a range from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class LongIndexingTests {
    private static final Logger logger = LogManager.getLogger(LongIndexingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public LongIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that long fields are indexed and stored correctly.
     * Long fields in OpenSearch can store values from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
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
            "9223372036854775807, 'Maximum long value (9223372036854775807) is indexed correctly'",
            "-9223372036854775808, 'Minimum long value (-9223372036854775808) is indexed correctly'",
            "-1, 'Negative value is indexed correctly'",
            "10000000000, 'Large positive value is indexed correctly'"
    })
    public void longMapping_IndexesNumericValueCorrectly(long stockValue, String explanation) throws Exception {
        // Create a test index with long mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.long_(l -> l))))) {

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
     * This test verifies that long fields maintain precision and do not overflow
     * when storing values at the boundaries of the long range.
     *
     * @param stockValue  The stock value to test
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "9223372036854775807, 'Maximum long value boundary test'",
            "-9223372036854775808, 'Minimum long value boundary test'"
    })
    public void longMapping_PreservesBoundaryValues(long stockValue, String explanation) throws Exception {
        // Create a test index with long mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.long_(l -> l))))) {

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

