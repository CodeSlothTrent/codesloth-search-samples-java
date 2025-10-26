package FloatDemo;

import FloatDemo.Documents.ProductDocument;
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
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for float field indexing in OpenSearch.
 * These tests demonstrate how float fields are indexed and stored in OpenSearch.
 *
 * Float fields store 32-bit IEEE 754 floating point values.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class FloatIndexingTests {
    private static final Logger logger = LogManager.getLogger(FloatIndexingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public FloatIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that float fields are indexed and stored correctly.
     * Float fields in OpenSearch store 32-bit floating point numbers.
     *
     * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
     *
     * @param priceValue  The price value to index
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "0.0, 'Zero value is indexed correctly'",
            "1.5, 'Positive decimal value is indexed correctly'",
            "99.99, 'Typical price value is indexed correctly'",
            "-25.50, 'Negative decimal value is indexed correctly'",
            "1000000.75, 'Large positive value is indexed correctly'"
    })
    public void floatMapping_IndexesNumericValueCorrectly(float priceValue, String explanation) throws Exception {
        // Create a test index with float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.float_(f -> f))))) {

            // Create and index a product document
            ProductDocument productDocument = new ProductDocument(1, "Mouse", priceValue);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Retrieve the document
            GetResponse<ProductDocument> result = openSearchClient.get(g -> g
                    .index(testIndex.getName())
                    .id(productDocument.getId()),
                    ProductDocument.class
            );

            // Verify the results (using tolerance for floating point comparison)
            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getPrice()).as(explanation).isCloseTo(priceValue, within(0.0001f));
            assertThat(result.source().getName()).isEqualTo("Mouse");
        }
    }

    /**
     * This test verifies that float fields maintain precision for typical decimal values.
     *
     * @param priceValue  The price value to test
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "19.99, 'Common price point'",
            "123.45, 'Decimal precision test'"
    })
    public void floatMapping_PreservesDecimalPrecision(float priceValue, String explanation) throws Exception {
        // Create a test index with float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.float_(f -> f))))) {

            // Create and index multiple documents to ensure consistent behavior
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Product A", priceValue),
                    new ProductDocument(2, "Product B", priceValue),
                    new ProductDocument(3, "Product C", priceValue)
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
                assertThat(result.source().getPrice())
                        .as(explanation + " for document " + doc.getId())
                        .isCloseTo(priceValue, within(0.0001f));
            }
        }
    }
}

