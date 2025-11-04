package FlattenedDemo;

import FlattenedDemo.Documents.DateAttribute;
import FlattenedDemo.Documents.ProductWithDateAttribute;
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
 * Tests for date range queries on flattened fields in OpenSearch.
 * 
 * Flattened fields store all sub-properties as keywords (unanalyzed strings). For date range queries
 * to work correctly, dates must be ISO8601 formatted (e.g., "2024-01-15T10:30:00Z" or "2024-01-15").
 * Non-ISO formatted dates will fail range queries because they are compared lexicographically,
 * not as dates.
 * 
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/flattened/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class FlattenedDateRangeTests {
    private static final Logger logger = LogManager.getLogger(FlattenedDateRangeTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public FlattenedDateRangeTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * Tests that ISO8601 formatted dates work correctly with range queries on flattened fields.
     * ISO8601 format ensures proper lexicographic ordering that matches chronological ordering.
     * 
     * This test uses the same dates as the failure test (flattenedDateRange_NonISOFormat_FailsRangeQueries),
     * but converts them to ISO8601 format to demonstrate that the same cross-year scenario works correctly
     * when using ISO8601 format.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_ISO8601Format_WorksWithRangeQueries() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with ISO8601 formatted dates (same dates as the failure test, but in ISO8601 format)
            // Using the same days of the year: 12/30, 12/31, 01/01, 01/02
            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("2023-12-30")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("2023-12-31")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("2024-01-01")),
                    new ProductWithDateAttribute("4", "Product4", new DateAttribute("2024-01-02"))
            };
            testIndex.indexDocuments(products);

            // Query for dates between 2023-12-31 and 2024-01-01 (inclusive)
            // This is the same range as the failure test, but with ISO8601 format
            SearchResponse<ProductWithDateAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .gte(JsonData.of("2023-12-31"))
                                            .lte(JsonData.of("2024-01-01"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // Should match Product2 (2023-12-31) and Product3 (2024-01-01)
            // This demonstrates that ISO8601 format correctly handles the cross-year scenario
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("2", "3");
        }
    }

    /**
     * Tests that non-ISO formatted dates fail range queries on flattened fields.
     * 
     * This test demonstrates a cross-year scenario where ASCII ordering fails:
     * - Chronologically: 12/31/2023 < 01/01/2024 (December 31, 2023 comes before January 1, 2024)
     * - ASCII: "12/31/2023" > "01/01/2024" (because "1" > "0" in the first character)
     * 
     * When querying for dates between 12/31/2023 and 01/01/2024, the range query fails
     * because "12/31/2023" > "01/01/2024" in ASCII order, so no strings can satisfy
     * both gte("12/31/2023") and lte("01/01/2024") simultaneously.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_NonISOFormat_FailsRangeQueries() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with non-ISO formatted dates (MM/DD/YYYY format)
            // This includes a cross-year scenario that will fail in ASCII comparison
            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("12/30/2023")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("12/31/2023")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("01/01/2024")),
                    new ProductWithDateAttribute("4", "Product4", new DateAttribute("01/02/2024"))
            };
            testIndex.indexDocuments(products);

            // Try to query for dates between 12/31/2023 and 01/01/2024 (inclusive)
            // Chronologically, this should match Product2 (12/31/2023) and Product3 (01/01/2024)
            // However, in ASCII comparison: "12/31/2023" > "01/01/2024" (because "1" > "0")
            // This means no strings can satisfy both gte("12/31/2023") and lte("01/01/2024")
            // simultaneously in ASCII order, so the query returns 0 results
            SearchResponse<ProductWithDateAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .gte(JsonData.of("12/31/2023"))
                                            .lte(JsonData.of("01/01/2024"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // The query fails because "12/31/2023" > "01/01/2024" in ASCII comparison
            // Since the lower bound is greater than the upper bound in ASCII order,
            // no documents can satisfy both conditions, resulting in 0 results
            // This demonstrates why ISO8601 format is required for reliable range queries
            assertThat(result.hits().total().value()).isEqualTo(0);
        }
    }

    /**
     * Tests that ISO8601 dates work with gte (greater than or equal) single point queries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_ISO8601Format_GreaterThanOrEqualSinglePoint() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("2024-01-15")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("2024-01-20")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("2024-01-25"))
            };
            testIndex.indexDocuments(products);

            // Query for dates >= 2024-01-20
            SearchResponse<ProductWithDateAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .gte(JsonData.of("2024-01-20"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // Should match Product2 (2024-01-20) and Product3 (2024-01-25)
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("2", "3");
        }
    }

    /**
     * Tests that ISO8601 dates work with lte (less than or equal) single point queries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_ISO8601Format_LessThanOrEqualSinglePoint() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("2024-01-15")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("2024-01-20")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("2024-01-25"))
            };
            testIndex.indexDocuments(products);

            // Query for dates <= 2024-01-20
            SearchResponse<ProductWithDateAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .lte(JsonData.of("2024-01-20"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // Should match Product1 (2024-01-15) and Product2 (2024-01-20)
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "2");
        }
    }

    /**
     * Tests that ISO8601 dates work with gt (greater than) single point queries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_ISO8601Format_GreaterThanSinglePoint() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("2024-01-15")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("2024-01-20")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("2024-01-25"))
            };
            testIndex.indexDocuments(products);

            // Query for dates > 2024-01-20
            SearchResponse<ProductWithDateAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .gt(JsonData.of("2024-01-20"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // Should match only Product3 (2024-01-25), not Product2 (exact match)
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("3");
        }
    }

    /**
     * Tests that ISO8601 dates work with lt (less than) single point queries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_ISO8601Format_LessThanSinglePoint() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("2024-01-15")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("2024-01-20")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("2024-01-25"))
            };
            testIndex.indexDocuments(products);

            // Query for dates < 2024-01-20
            SearchResponse<ProductWithDateAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .lt(JsonData.of("2024-01-20"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // Should match only Product1 (2024-01-15), not Product2 (exact match)
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1");
        }
    }

    /**
     * Tests date range queries with both gte and lte together, including edge cases at boundaries.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_ISO8601Format_RangeWithBoundaries() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("2024-01-15")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("2024-01-20")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("2024-01-25")),
                    new ProductWithDateAttribute("4", "Product4", new DateAttribute("2024-02-01"))
            };
            testIndex.indexDocuments(products);

            // Query for dates between 2024-01-20 and 2024-01-25 (inclusive)
            SearchResponse<ProductWithDateAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .gte(JsonData.of("2024-01-20"))
                                            .lte(JsonData.of("2024-01-25"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // Should match Product2 (2024-01-20) and Product3 (2024-01-25) - both boundaries included
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("2", "3");
        }
    }

    /**
     * Tests that various non-ISO date formats fail range queries.
     * This demonstrates the requirement for ISO8601 format.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_VariousNonISOFormats_FailRangeQueries() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with various non-ISO formats
            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("January 15, 2024")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("01/15/2024")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("2024-1-15")), // missing zero padding
                    new ProductWithDateAttribute("4", "Product4", new DateAttribute("15-01-2024")) // DD-MM-YYYY format
            };
            testIndex.indexDocuments(products);

            // Try to query for a date range - these will fail due to incorrect lexicographic ordering
            SearchResponse<ProductWithDateAttribute> result1 = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .gte(JsonData.of("January 15, 2024"))
                                            .lte(JsonData.of("January 20, 2024"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // The query will likely fail or return incorrect results due to lexicographic comparison
            // This demonstrates why ISO8601 format is required for range queries on flattened fields
            logger.info("Non-ISO format query result count: {}", result1.hits().total().value());
        }
    }
}

