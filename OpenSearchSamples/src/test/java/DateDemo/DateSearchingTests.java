package DateDemo;

import DateDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.json.JsonData;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for date field searching in OpenSearch.
 * These tests demonstrate how date fields can be queried, including range queries.
 *
 * Date fields support term queries and range queries for date/time comparisons.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/date/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class DateSearchingTests {
    private static final Logger logger = LogManager.getLogger(DateSearchingTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public DateSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that term queries on date fields match exact date values.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void dateMapping_ExactlyMatchesTermQuery() throws Exception {
        // Create a test index with date mapping for the createdAt field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("createdAt", Property.of(p -> p.date(d -> d))))) {

            String targetDate = "2024-06-15T12:00:00Z";
            
            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", "2024-01-15T10:00:00Z"),
                    new ProductDocument(2, "Keyboard", targetDate),
                    new ProductDocument(3, "Monitor", "2024-12-15T14:00:00Z")
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a term query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("createdAt")
                                            .value(FieldValue.of(targetDate))
                                    )
                            ),
                    ProductDocument.class);

            // Verify the results
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getId()).isEqualTo("2");
        }
    }

    /**
     * Test data provider for range query tests.
     * Each test case specifies the range boundaries, expected matching document IDs, and explanation.
     */
    private static Stream<org.junit.jupiter.params.provider.Arguments> rangeQueryTestCases() {
        return Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(
                        "2024-01-01T00:00:00Z",
                        "2024-06-30T23:59:59Z",
                        2L,
                        List.of("1", "2"),
                        "Range [2024-01-01 to 2024-06-30] matches: Mouse(id=1,createdAt=2024-01-15), Keyboard(id=2,createdAt=2024-06-15)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        "2024-06-15T12:00:00Z",
                        "2024-06-15T12:00:00Z",
                        1L,
                        List.of("2"),
                        "Range [2024-06-15 12:00:00, exact] matches: Keyboard(id=2,createdAt=2024-06-15T12:00:00Z)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        "2024-07-01T00:00:00Z",
                        "2024-12-31T23:59:59Z",
                        1L,
                        List.of("3"),
                        "Range [2024-07-01 to 2024-12-31] matches: Monitor(id=3,createdAt=2024-12-15)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        "2024-01-01T00:00:00Z",
                        "2024-12-31T23:59:59Z",
                        3L,
                        List.of("1", "2", "3"),
                        "Range [2024-01-01 to 2024-12-31] matches all documents"
                )
        );
    }

    /**
     * This test verifies that date fields support range queries for date comparisons.
     * The test documents are:
     * - id=1: Mouse, createdAt=2024-01-15T10:00:00Z
     * - id=2: Keyboard, createdAt=2024-06-15T12:00:00Z
     * - id=3: Monitor, createdAt=2024-12-15T14:00:00Z
     *
     * @param gte            Greater than or equal to date (ISO-8601 string)
     * @param lte            Less than or equal to date (ISO-8601 string)
     * @param expectedHits   Expected number of matching documents
     * @param expectedDocIds List of expected document IDs
     * @param explanation    The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.MethodSource("rangeQueryTestCases")
    public void dateMapping_SupportsRangeQueries(String gte, String lte, long expectedHits, List<String> expectedDocIds, String explanation) throws Exception {
        // Create a test index with date mapping for the createdAt field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("createdAt", Property.of(p -> p.date(d -> d))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", "2024-01-15T10:00:00Z"),
                    new ProductDocument(2, "Keyboard", "2024-06-15T12:00:00Z"),
                    new ProductDocument(3, "Monitor", "2024-12-15T14:00:00Z")
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a range query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("createdAt")
                                            .gte(JsonData.of(gte))
                                            .lte(JsonData.of(lte))
                                    )
                            ),
                    ProductDocument.class);

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(expectedHits);
            assertThat(result.hits().hits()).hasSize((int) expectedHits);

            // Verify the specific document IDs that matched
            List<String> actualIds = result.hits().hits().stream()
                    .map(hit -> hit.source().getId())
                    .sorted()
                    .collect(Collectors.toList());

            List<String> expectedIdsSorted = expectedDocIds.stream()
                    .sorted()
                    .collect(Collectors.toList());

            assertThat(actualIds).as("Matched document IDs for " + explanation).isEqualTo(expectedIdsSorted);
        }
    }

    /**
     * This test verifies that date fields can be filtered using boolean queries.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void dateMapping_CanBeFilteredOnWithBooleanQuery() throws Exception {
        // Create a test index with date mapping for the createdAt field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("createdAt", Property.of(p -> p.date(d -> d))))) {

            String targetDate = "2024-06-15T12:00:00Z";
            
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", "2024-01-15T10:00:00Z"),
                    new ProductDocument(2, "Keyboard", targetDate)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a boolean query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .bool(b -> b
                                            .filter(f -> f
                                                    .term(t -> t
                                                            .field("createdAt")
                                                            .value(FieldValue.of(targetDate))
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class);

            // Verify the results
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getId()).isEqualTo("2");
        }
    }
}

