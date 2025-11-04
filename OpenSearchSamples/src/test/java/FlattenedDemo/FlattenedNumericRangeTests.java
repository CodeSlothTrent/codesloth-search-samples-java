package FlattenedDemo;

import FlattenedDemo.Documents.NumericAttribute;
import FlattenedDemo.Documents.ProductWithNumericAttribute;
import TestExtensions.LoggingOpenSearchClient;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.json.JsonData;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for numeric range queries on flattened fields in OpenSearch.
 * 
 * Flattened fields store all sub-properties as keywords (unanalyzed strings). For numeric range queries
 * to work correctly, numeric values must be zero-padded to the same length because they are compared
 * lexicographically (ASCII), not numerically. This means:
 * - Unpadded numbers (e.g., "1", "10", "100") will not work correctly - "10" < "2" in ASCII comparison
 * - Zero-padded numbers (e.g., "0001", "0010", "0100") will work correctly
 * - All numbers must be padded to the maximum size (up to Integer.MAX_VALUE = 2,147,483,647 = 10 digits)
 * - Negative numbers may have additional complications
 * 
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/flattened/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class FlattenedNumericRangeTests {
    private static final Logger logger = LogManager.getLogger(FlattenedNumericRangeTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public FlattenedNumericRangeTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * Tests that unpadded numbers fail range queries on flattened fields.
     * ASCII comparison means "10" < "2" (because "1" < "2" in ASCII), which breaks numeric ordering.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_UnpaddedNumbers_FailsRangeQueries() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with unpadded numbers
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("1")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("2")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("10")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("100")),
                    new ProductWithNumericAttribute("5", "Product5", new NumericAttribute("1000"))
            };
            testIndex.indexDocuments(products);

            // Try to query for values between "1" and "10"
            // This will fail because "10" < "2" in ASCII comparison
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of("1"))
                                            .lte(JsonData.of("10"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // The query will fail because ASCII comparison order is: "1", "10", "100", "1000", "2"
            // So "1" <= x <= "10" matches "1", "10", "100", "1000" but not "2"
            // This demonstrates why zero-padding is required
            assertThat(result.hits().total().value()).isNotEqualTo(3); // Expected 3 (1, 2, 10) but will be wrong
            logger.info("Unpadded numbers query result count: {} (expected 3, but will be incorrect due to ASCII comparison)", 
                    result.hits().total().value());
        }
    }

    /**
     * Tests that zero-padded numbers work correctly with range queries on flattened fields.
     * When all numbers are padded to the same length, ASCII comparison matches numeric comparison.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_ZeroPaddedNumbers_WorksWithRangeQueries() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with zero-padded numbers (4 digits)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("0001")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("0002")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("0010")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("0100")),
                    new ProductWithNumericAttribute("5", "Product5", new NumericAttribute("1000"))
            };
            testIndex.indexDocuments(products);

            // Query for values between "0001" and "0010" (inclusive)
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of("0001"))
                                            .lte(JsonData.of("0010"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Should match Product1 (0001), Product2 (0002), and Product3 (0010)
            assertThat(result.hits().total().value()).isEqualTo(3);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "2", "3");
        }
    }

    /**
     * Tests that all numbers must be padded to the maximum size (up to Integer.MAX_VALUE = 10 digits).
     * This demonstrates the limitation that all numbers must be padded to the same maximum length
     * to support the full integer range.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_MaxSizePadding_RequiredForFullIntegerRange() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with numbers padded to 10 digits (max size for Integer.MAX_VALUE = 2,147,483,647)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("0000000001")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("0000000002")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("0000000010")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("0000000100")),
                    new ProductWithNumericAttribute("5", "Product5", new NumericAttribute("0000001000")),
                    new ProductWithNumericAttribute("6", "Product6", new NumericAttribute("2147483647")) // Integer.MAX_VALUE
            };
            testIndex.indexDocuments(products);

            // Query for values between "0000000001" and "0000000100" (inclusive)
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of("0000000001"))
                                            .lte(JsonData.of("0000000100"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Should match Product1 (0000000001), Product2 (0000000002), Product3 (0000000010), and Product4 (0000000100)
            assertThat(result.hits().total().value()).isEqualTo(4);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "2", "3", "4");

            // Query for Integer.MAX_VALUE
            SearchResponse<ProductWithNumericAttribute> maxResult = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of("2147483647"))
                                            .lte(JsonData.of("2147483647"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Should match Product6 (Integer.MAX_VALUE)
            assertThat(maxResult.hits().total().value()).isEqualTo(1);
            assertThat(maxResult.hits().hits().get(0).source().getId()).isEqualTo("6");
        }
    }

    /**
     * Tests negative numbers with zero-padding.
     * Negative numbers have a "-" prefix, which affects ASCII comparison.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_NegativeNumbers_WithZeroPadding() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with negative numbers (various padding approaches)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("-1")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("-10")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("-001")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("-0001")),
                    new ProductWithNumericAttribute("5", "Product5", new NumericAttribute("-0000000001"))
            };
            testIndex.indexDocuments(products);

            // Query for negative values
            // Note: In ASCII comparison, "-0000000001" < "-0001" < "-001" < "-1" < "-10"
            // This is because "-" comes before digits, so "-0000000001" is the smallest
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of("-10"))
                                            .lte(JsonData.of("-1"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // In ASCII comparison: "-1" < "-10" (because "-1" < "-10" character by character)
            // So "-10" <= x <= "-1" will match "-1" and "-10"
            // But this is incorrect numerically (we expect -10 to -1)
            logger.info("Negative numbers query result count: {}", result.hits().total().value());
            logger.info("This demonstrates that negative numbers have complex ASCII comparison behavior");
        }
    }

    /**
     * Tests that zero-padded numbers work with gte (greater than or equal) single point queries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_ZeroPaddedNumbers_GreaterThanOrEqualSinglePoint() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with zero-padded numbers (4 digits)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("0001")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("0002")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("0010")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("0100"))
            };
            testIndex.indexDocuments(products);

            // Query for values >= 0002
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of("0002"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Should match Product2 (0002), Product3 (0010), and Product4 (0100)
            assertThat(result.hits().total().value()).isEqualTo(3);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("2", "3", "4");
        }
    }

    /**
     * Tests that zero-padded numbers work with lte (less than or equal) single point queries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_ZeroPaddedNumbers_LessThanOrEqualSinglePoint() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with zero-padded numbers (4 digits)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("0001")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("0002")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("0010")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("0100"))
            };
            testIndex.indexDocuments(products);

            // Query for values <= 0002
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .lte(JsonData.of("0002"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Should match Product1 (0001) and Product2 (0002)
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "2");
        }
    }

    /**
     * Tests that zero-padded numbers work with gt (greater than) single point queries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_ZeroPaddedNumbers_GreaterThanSinglePoint() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with zero-padded numbers (4 digits)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("0001")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("0002")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("0010")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("0100"))
            };
            testIndex.indexDocuments(products);

            // Query for values > 0002
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gt(JsonData.of("0002"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Should match only Product3 (0010) and Product4 (0100), not Product2 (exact match)
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("3", "4");
        }
    }

    /**
     * Tests that zero-padded numbers work with lt (less than) single point queries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_ZeroPaddedNumbers_LessThanSinglePoint() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with zero-padded numbers (4 digits)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("0001")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("0002")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("0010")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("0100"))
            };
            testIndex.indexDocuments(products);

            // Query for values < 0002
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .lt(JsonData.of("0002"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Should match only Product1 (0001), not Product2 (exact match)
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits().get(0).source().getId()).isEqualTo("1");
        }
    }

    /**
     * Tests that mixing padded and unpadded numbers causes incorrect results.
     * This demonstrates why all numbers must be consistently padded.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_MixedPadding_CausesIncorrectResults() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with mixed padding (some padded, some not)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("1")),      // unpadded
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("0002")),   // padded to 4 digits
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("10")),      // unpadded
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("0010"))     // padded to 4 digits
            };
            testIndex.indexDocuments(products);

            // Query for values between "1" and "10"
            // This will fail because mixing padding causes incorrect ASCII comparison
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of("1"))
                                            .lte(JsonData.of("10"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // The results will be incorrect due to mixed padding
            // ASCII order: "0002" < "0010" < "1" < "10"
            // So "1" <= x <= "10" matches "1", "10", but also "0002" and "0010" (which are numerically 2 and 10)
            logger.info("Mixed padding query result count: {} (demonstrates incorrect results)", 
                    result.hits().total().value());
            assertThat(result.hits().total().value()).isNotEqualTo(3); // Expected 3 (1, 2, 10) but will be wrong
        }
    }
}
