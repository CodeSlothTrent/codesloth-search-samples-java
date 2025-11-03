package NumericFieldDataTypes.LongDemo;

import NumericFieldDataTypes.LongDemo.Documents.ProductDocument;
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
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.json.JsonData;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for long field searching in OpenSearch.
 * These tests demonstrate how long fields can be queried using various query types.
 *
 * Long fields support term-level queries and range queries suitable for numeric data.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class LongSearchingTests {
    private static final Logger logger = LogManager.getLogger(LongSearchingTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public LongSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that term queries on long fields match exact numeric values.
     *
     * @param stockValue  The stock value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "100000, 'Matches documents with exact stock value of 100000'",
            "50000000000, 'Matches documents with exact stock value of 50000000000'",
            "9223372036854775807, 'Matches documents with maximum long value (9223372036854775807)'",
            "-9223372036854775808, 'Matches documents with minimum long value (-9223372036854775808)'"
    })
    public void longMapping_ExactlyMatchesTermQuery(long stockValue, String explanation) throws Exception {
        // Create a test index with long mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.long_(l -> l))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 100000L),
                    new ProductDocument(2, "Keyboard", 50000000000L),
                    new ProductDocument(3, "Monitor", 9223372036854775807L),
                    new ProductDocument(4, "Cable", -9223372036854775808L)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a term query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("stock")
                                            .value(FieldValue.of(stockValue))
                                    )
                            ),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getStock()).isEqualTo(stockValue);
        }
    }

    /**
     * This test verifies that long fields can be filtered using boolean queries.
     *
     * @param stockValue  The stock value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "100000, 'Boolean filter matches stock value of 100000'",
            "50000000000, 'Boolean filter matches stock value of 50000000000'"
    })
    public void longMapping_CanBeFilteredOnWithBooleanQuery(long stockValue, String explanation) throws Exception {
        // Create a test index with long mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.long_(l -> l))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 100000L),
                    new ProductDocument(2, "Keyboard", 50000000000L)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a boolean query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .bool(b -> b
                                            .filter(f -> f
                                                    .term(t -> t
                                                            .field("stock")
                                                            .value(FieldValue.of(stockValue))
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getStock()).isEqualTo(stockValue);
        }
    }

    /**
     * This test verifies that long fields can be queried using constant score queries.
     *
     * @param stockValue  The stock value to search for
     * @param boostValue  The boost value to apply
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "100000, 2.0, 'Constant score query with boost 2.0'",
            "50000000000, 5.0, 'Constant score query with boost 5.0'"
    })
    public void longMapping_CanBeFilteredAndScoredOnWithConstantScoreQuery(long stockValue, float boostValue, String explanation) throws Exception {
        // Create a test index with long mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.long_(l -> l))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 100000L),
                    new ProductDocument(2, "Keyboard", 50000000000L)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a constant score query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .constantScore(cs -> cs
                                            .filter(f -> f
                                                    .term(t -> t
                                                            .field("stock")
                                                            .value(FieldValue.of(stockValue))
                                                    )
                                            )
                                            .boost(boostValue)
                                    )
                            ),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getStock()).isEqualTo(stockValue);
            assertThat(result.hits().hits().get(0).score()).isEqualTo(boostValue);
        }
    }

    /**
     * Test data provider for range query tests.
     * Each test case specifies the range boundaries, expected matching document IDs, and explanation.
     */
    private static Stream<org.junit.jupiter.params.provider.Arguments> rangeQueryTestCases() {
        return Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(
                        0L, 50000000000L, 3L, List.of("1", "2", "5"),
                        "Range [0,50000000000] matches: Mouse(id=1,stock=100000), Keyboard(id=2,stock=50000000000), Headset(id=5,stock=0)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        100000L, 100000L, 1L, List.of("1"),
                        "Range [100000,100000] matches exact value: Mouse(id=1,stock=100000)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        50000000001L, 9223372036854775807L, 1L, List.of("3"),
                        "Range [50000000001,9223372036854775807] matches: Monitor(id=3,stock=100000000000)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        -9223372036854775808L, 0L, 2L, List.of("4", "5"),
                        "Range [-9223372036854775808,0] matches: Cable(id=4,stock=-50000000000), Headset(id=5,stock=0)"
                )
        );
    }

    /**
     * This test verifies that long fields support range queries for numeric comparisons.
     * The test documents are:
     * - id=1: Mouse, stock=100000
     * - id=2: Keyboard, stock=50000000000
     * - id=3: Monitor, stock=100000000000
     * - id=4: Cable, stock=-50000000000
     * - id=5: Headset, stock=0
     *
     * @param gte              Greater than or equal to value
     * @param lte              Less than or equal to value
     * @param expectedHits     Expected number of matching documents
     * @param expectedDocIds   List of expected document IDs
     * @param explanation      The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @MethodSource("rangeQueryTestCases")
    public void longMapping_SupportsRangeQueries(long gte, long lte, long expectedHits, List<String> expectedDocIds, String explanation) throws Exception {
        // Create a test index with long mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.long_(l -> l))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 100000L),
                    new ProductDocument(2, "Keyboard", 50000000000L),
                    new ProductDocument(3, "Monitor", 100000000000L),
                    new ProductDocument(4, "Cable", -50000000000L),
                    new ProductDocument(5, "Headset", 0L)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a range query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("stock")
                                            .gte(JsonData.of(gte))
                                            .lte(JsonData.of(lte))
                                    )
                            ),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(expectedHits);
            assertThat(result.hits().hits()).hasSize((int) expectedHits);

            // Verify the specific document IDs that matched (including any duplicates)
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
     * This test verifies that match queries on long fields work correctly.
     * Match queries are typically for full-text search, but they also work with numeric fields
     * by converting the query value to the appropriate numeric type.
     *
     * @param stockValue  The stock value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "100000, 'Match query finds exact long value 100000'",
            "50000000000, 'Match query finds exact long value 50000000000'"
    })
    public void longMapping_WorksWithMatchQuery(long stockValue, String explanation) throws Exception {
        // Create a test index with long mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.long_(l -> l))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 100000L),
                    new ProductDocument(2, "Keyboard", 50000000000L)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a match query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .match(m -> m
                                            .field("stock")
                                            .query(FieldValue.of(stockValue))
                                    )
                            ),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getStock()).isEqualTo(stockValue);
        }
    }
}

