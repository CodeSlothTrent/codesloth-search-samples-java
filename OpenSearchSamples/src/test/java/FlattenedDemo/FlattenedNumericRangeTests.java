package FlattenedDemo;

import FlattenedDemo.Documents.NumericAttribute;
import FlattenedDemo.Documents.ProductWithNumericAttribute;
import TestExtensions.LoggingOpenSearchClient;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.json.JsonData;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for numeric range queries on flattened fields in OpenSearch.
 * <p>
 * Flattened fields store all sub-properties as keywords (unanalyzed strings). For numeric range queries
 * to work correctly, numeric values must be zero-padded to the same length because they are compared
 * lexicographically (ASCII), not numerically.
 * <p>
 * Key behaviors:
 * <ul>
 *   <li><strong>Unpadded numbers fail:</strong> Range queries on unpadded numbers (e.g., "1", "10", "100")
 *       fail because lexicographic comparison doesn't match numeric ordering. For example, a range ["1", "10"]
 *       will only match "1" and "10", excluding "100" (which is lexicographically &gt; "10") and "2"
 *       (which is also lexicographically &gt; "10").</li>
 *   <li><strong>Zero-padded numbers work:</strong> When all numbers are padded to the same length
 *       (e.g., "0001", "0010", "0100"), lexicographic comparison matches numeric comparison.</li>
 *   <li><strong>Maximum padding required:</strong> All numbers must be padded to the maximum size
 *       (up to Integer.MAX_VALUE = 2,147,483,647 = 10 digits) to support the full integer range.</li>
 *   <li><strong>Negative numbers:</strong> Zero-padding alone doesn't solve negative number ranges that
 *       span from negative to positive values. A two's complement approach (adding |Integer.MIN_VALUE|)
 *       converts all values to positive, enabling correct range queries.</li>
 *   <li><strong>Mixed padding fails:</strong> Mixing padded and unpadded numbers causes incorrect results
 *       because padded values may fall outside the lexicographic range bounds.</li>
 * </ul>
 * <p>
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/flattened/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class FlattenedNumericRangeTests {
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
     * <p>
     * When querying for a range ["1", "10"] on unpadded numbers, the query fails because:
     * <ul>
     *   <li>ASCII comparison order is: "1", "10", "100", "1000", "2"</li>
     *   <li>The range query ["1", "10"] lexicographically matches only "1" and "10"</li>
     *   <li>Values like "100" and "1000" are excluded because they're lexicographically &gt; "10"</li>
     *   <li>The value "2" is excluded because it's lexicographically &gt; "10"</li>
     * </ul>
     * Numerically, we expect 3 results (1, 2, 10), but only 2 are returned (1, 10).
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
            // So "1" <= x <= "10" matches only "1" and "10" (not "100" or "1000" since they're > "10" lexicographically)
            // Expected 3 (1, 2, 10) but will be wrong due to ASCII comparison
            assertThat(result.hits().total().value()).isNotEqualTo(3);
            
            // Verify the actual incorrect results: matches only "1" and "10", missing "2", "100", and "1000"
            List<String> matchedIds = result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted()
                    .collect(Collectors.toList());
            assertThat(matchedIds).containsExactly("1", "3"); // Missing "2", "4" (100), and "5" (1000)!
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
     * Tests that unpadded negative numbers fail range queries on flattened fields.
     * Negative numbers have a "-" prefix, which affects ASCII comparison.
     * Unpadded negative numbers suffer from the same ASCII ordering issues as positive numbers.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_UnpaddedNegativeNumbers_FailsRangeQueries() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with unpadded negative numbers
            // ASCII order: "-1", "-10", "-100", "-2", "0", "1", "10" (partial order)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("-100")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("-10")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("-2")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("-1")),
                    new ProductWithNumericAttribute("5", "Product5", new NumericAttribute("0")),
                    new ProductWithNumericAttribute("6", "Product6", new NumericAttribute("1")),
                    new ProductWithNumericAttribute("7", "Product7", new NumericAttribute("10"))
            };
            testIndex.indexDocuments(products);

            // Query for range [-10, 10] with unpadded numbers
            // The query fails because ASCII comparison doesn't match numeric ordering for negative numbers
            // Result: Matches "-10", "-100", "-2", "0", "1", "10" but incorrectly excludes "-1"
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of("-10"))
                                            .lte(JsonData.of("10"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // Demonstrates the failure with unpadded negative numbers
            assertThat(result.hits().total().value()).isEqualTo(6); // "-10", "-100", "-2", "0", "1", "10"
            
            List<String> matchedIds = result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted()
                    .collect(Collectors.toList());

            // "-1" is missing even though it should be in the range [-10, 10] numerically
            // This demonstrates the unpredictable behavior with unpadded negative numbers
            assertThat(matchedIds).containsExactly("1", "2", "3", "5", "6", "7"); // Missing "4" (-1)!
        }
    }

    /**
     * Tests zero-padded negative numbers with range queries on flattened fields.
     * This test demonstrates that zero-padding alone does NOT solve the problem for negative numbers
     * when the range spans from negative to positive values, due to ASCII comparison issues.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_ZeroPaddedNegativeNumbers_StillFailsForMixedRanges() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with zero-padded negative numbers (4 digits after minus sign)
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute("-0100")),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute("-0010")),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute("-0002")),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute("-0001")),
                    new ProductWithNumericAttribute("5", "Product5", new NumericAttribute("0000")),
                    new ProductWithNumericAttribute("6", "Product6", new NumericAttribute("0001")),
                    new ProductWithNumericAttribute("7", "Product7", new NumericAttribute("0010"))
            };
            testIndex.indexDocuments(products);

            // Query for range [-10, 10] with padded numbers
            // Zero-padding alone doesn't solve the problem: "-0002" and "-0001" are < "-0010" in ASCII
            // Result: Matches "-0010", "-0100", "0000", "0001", "0010" but incorrectly excludes "-0002" and "-0001"
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of("-0010"))
                                            .lte(JsonData.of("0010"))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // The result shows that even with zero-padding, negative numbers don't work correctly
            // The query matches "-0010", "-0100", "0000", "0001", "0010" but misses "-0002" and "-0001"
            assertThat(result.hits().total().value()).isEqualTo(5); // "-0010", "-0100", "0000", "0001", "0010"
            
            List<String> matchedIds = result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted()
                    .collect(Collectors.toList());

            // Missing "-0002" (ID 3) and "-0001" (ID 4) because they are < "-0010" in ASCII
            // This demonstrates that zero-padding doesn't fully solve the problem for negative numbers
            assertThat(matchedIds).containsExactly("1", "2", "5", "6", "7"); // Missing "3" (-0002) and "4" (-0001)!
        }
    }

    /**
     * Tests that two's complement approach works correctly with range queries on flattened fields.
     * By adding Integer.MIN_VALUE (2,147,483,648) to all values, negatives become positive and
     * all values can be stored as zero-padded strings without signs, ensuring correct ASCII ordering.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedNumericRange_TwosComplement_WorksWithRangeQueries() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Two's complement offset: Integer.MIN_VALUE absolute value = 2,147,483,648
            // This converts all negative numbers to positive, ensuring correct ASCII ordering
            // All values are zero-padded to 10 digits (max value after offset: 4,294,967,295)
            final long offset = 2147483648L; // |Integer.MIN_VALUE|
            
            ProductWithNumericAttribute[] products = new ProductWithNumericAttribute[]{
                    new ProductWithNumericAttribute("1", "Product1", new NumericAttribute(String.format("%010d", -100 + offset))),
                    new ProductWithNumericAttribute("2", "Product2", new NumericAttribute(String.format("%010d", -10 + offset))),
                    new ProductWithNumericAttribute("3", "Product3", new NumericAttribute(String.format("%010d", -2 + offset))),
                    new ProductWithNumericAttribute("4", "Product4", new NumericAttribute(String.format("%010d", -1 + offset))),
                    new ProductWithNumericAttribute("5", "Product5", new NumericAttribute(String.format("%010d", 0 + offset))),
                    new ProductWithNumericAttribute("6", "Product6", new NumericAttribute(String.format("%010d", 1 + offset))),
                    new ProductWithNumericAttribute("7", "Product7", new NumericAttribute(String.format("%010d", 10 + offset)))
            };
            testIndex.indexDocuments(products);

            // Query for range [-10, 10] using two's complement offset values
            // Convert range bounds to offset values: -10 + offset and 10 + offset
            SearchResponse<ProductWithNumericAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.value")
                                            .gte(JsonData.of(String.format("%010d", -10 + offset)))
                                            .lte(JsonData.of(String.format("%010d", 10 + offset)))
                                    )
                            ),
                    ProductWithNumericAttribute.class
            );

            // All values in range [-10, 10] are correctly included
            assertThat(result.hits().total().value()).isEqualTo(6); // -10, -2, -1, 0, 1, 10 (excludes -100)
            
            List<String> matchedIds = result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted()
                    .collect(Collectors.toList());

            // All values in range are correctly included
            // Note: Product1 (-100) is excluded because it's outside the range [-10, 10]
            assertThat(matchedIds).containsExactly("2", "3", "4", "5", "6", "7");
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
     * <p>
     * When mixing padding formats, lexicographic comparison breaks range queries:
     * <ul>
     *   <li>ASCII order: "0002" &lt; "0010" &lt; "1" &lt; "10"</li>
     *   <li>A range query ["1", "10"] only matches "1" and "10"</li>
     *   <li>Padded values "0002" and "0010" are excluded because they're lexicographically &lt; "1"</li>
     *   <li>Numeric values 2 and 10 (as padded strings) are incorrectly excluded</li>
     * </ul>
     * This demonstrates why all numbers must be consistently padded to the same length.
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
            // So "1" <= x <= "10" matches only "1" and "10" (not "0002" or "0010" since they're < "1" lexicographically)
            // Expected 3 (1, 2, 10) but will be wrong due to mixed padding
            assertThat(result.hits().total().value()).isNotEqualTo(3);
            
            // Verify the actual incorrect results: matches only "1" and "10", missing "2" and "4" (padded values)
            List<String> matchedIds = result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted()
                    .collect(Collectors.toList());
            assertThat(matchedIds).containsExactly("1", "3"); // Missing "2" (0002) and "4" (0010) due to mixed padding!
        }
    }

    /**
     * Tests two's complement conversion for different integer values.
     * Demonstrates converting numbers to two's complement representation and back again.
     * Two's complement is used to convert signed integers to unsigned representation
     * by adding the offset (|Integer.MIN_VALUE| = 2,147,483,648) to all values.
     * 
     * @param originalValueString The original integer value as a string
     * @param expectedTwoComplementString The expected two's complement value as a string
     */
    @ParameterizedTest
    @CsvSource({
        "-2147483648, 0",                    // Integer.MIN_VALUE
        "2147483647, 4294967295",            // Integer.MAX_VALUE
        "-2, 2147483646",                    // Negative number
        "2, 2147483650",                     // Positive number
        "0, 2147483648"                      // Zero
    })
    public void flattenedNumericRange_TwosComplementConversion_ConvertsCorrectly(
            String originalValueString, String expectedTwoComplementString) {
        
        // Two's complement offset: |Integer.MIN_VALUE| = 2,147,483,648
        final long offset = Math.abs((long)Integer.MIN_VALUE);
        
        // Convert string input to integer
        int originalValue = Integer.parseInt(originalValueString);
        
        // Convert to two's complement representation
        long twoComplementValue = originalValue + offset;
        
        // Convert expected string to long for comparison
        long expectedTwoComplement = Long.parseLong(expectedTwoComplementString);
        
        // Assert that the conversion matches the expected value
        assertThat(twoComplementValue).as("Two's complement conversion for %s", originalValueString)
                .isEqualTo(expectedTwoComplement);
        
        // Convert back from two's complement to original value
        int convertedBackValue = (int)(twoComplementValue - offset);
        
        // Assert that converting back matches the original value
        assertThat(convertedBackValue).as("Converted back value for %s", originalValueString)
                .isEqualTo(originalValue);
    }
}
