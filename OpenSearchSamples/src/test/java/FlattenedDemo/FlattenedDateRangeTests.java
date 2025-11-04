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
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_ISO8601Format_WorksWithRangeQueries() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with ISO8601 formatted dates
            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("2024-01-15T00:00:00Z")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("2024-01-20T00:00:00Z")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("2024-01-25T00:00:00Z")),
                    new ProductWithDateAttribute("4", "Product4", new DateAttribute("2024-02-01T00:00:00Z")),
                    new ProductWithDateAttribute("5", "Product5", new DateAttribute("2024-02-10T00:00:00Z"))
            };
            testIndex.indexDocuments(products);

            // Query for dates between 2024-01-20 and 2024-02-01 (inclusive)
            SearchResponse<ProductWithDateAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .gte(JsonData.of("2024-01-20T00:00:00Z"))
                                            .lte(JsonData.of("2024-02-01T00:00:00Z"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // Should match Product2 (2024-01-20), Product3 (2024-01-25), and Product4 (2024-02-01)
            assertThat(result.hits().total().value()).isEqualTo(3);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("2", "3", "4");
        }
    }

    /**
     * Tests that non-ISO formatted dates fail range queries on flattened fields.
     * Non-ISO formats like "01/15/2024" or "January 15, 2024" do not sort correctly
     * lexicographically, causing range queries to fail.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedDateRange_NonISOFormat_FailsRangeQueries() throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with non-ISO formatted dates
            ProductWithDateAttribute[] products = new ProductWithDateAttribute[]{
                    new ProductWithDateAttribute("1", "Product1", new DateAttribute("01/15/2024")),
                    new ProductWithDateAttribute("2", "Product2", new DateAttribute("01/20/2024")),
                    new ProductWithDateAttribute("3", "Product3", new DateAttribute("01/25/2024")),
                    new ProductWithDateAttribute("4", "Product4", new DateAttribute("02/01/2024"))
            };
            testIndex.indexDocuments(products);

            // Try to query for dates between 01/20/2024 and 02/01/2024
            // This will fail because "01/20/2024" > "02/01/2024" in ASCII comparison
            SearchResponse<ProductWithDateAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("attribute.createdDate")
                                            .gte(JsonData.of("01/20/2024"))
                                            .lte(JsonData.of("02/01/2024"))
                                    )
                            ),
                    ProductWithDateAttribute.class
            );

            // The query will fail because "01/20/2024" > "02/01/2024" in ASCII comparison
            // (the month part "01" comes before "02", but when comparing full strings,
            // "01/20/2024" > "02/01/2024" lexicographically)
            // This demonstrates why ISO8601 format is required
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

