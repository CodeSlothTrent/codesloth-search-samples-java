package NumericFieldDataTypes.ShortDemo;

import NumericFieldDataTypes.ShortDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.json.JsonData;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for short field searching in OpenSearch.
 * These tests demonstrate how short fields can be queried using various query types.
 *
 * Short fields support term-level queries and range queries suitable for numeric data.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class ShortSearchingTests {
    private static final Logger logger = LogManager.getLogger(ShortSearchingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public ShortSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that term queries on short fields match exact numeric values.
     *
     * @param stockValue  The stock value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "100, 'Matches documents with exact stock value of 100'",
            "5000, 'Matches documents with exact stock value of 5000'",
            "32767, 'Matches documents with maximum short value (32767)'",
            "-32768, 'Matches documents with minimum short value (-32768)'"
    })
    public void shortMapping_ExactlyMatchesTermQuery(short stockValue, String explanation) throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) 100),
                    new ProductDocument(2, "Keyboard", (short) 5000),
                    new ProductDocument(3, "Monitor", (short) 32767),
                    new ProductDocument(4, "Cable", (short) -32768)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a term query
            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
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
     * This test verifies that short fields can be filtered using boolean queries.
     *
     * @param stockValue  The stock value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "100, 'Boolean filter matches stock value of 100'",
            "5000, 'Boolean filter matches stock value of 5000'"
    })
    public void shortMapping_CanBeFilteredOnWithBooleanQuery(short stockValue, String explanation) throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) 100),
                    new ProductDocument(2, "Keyboard", (short) 5000)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a boolean query
            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
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
     * This test verifies that short fields can be queried using constant score queries.
     *
     * @param stockValue  The stock value to search for
     * @param boostValue  The boost value to apply
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "100, 2.0, 'Constant score query with boost 2.0'",
            "5000, 5.0, 'Constant score query with boost 5.0'"
    })
    public void shortMapping_CanBeFilteredAndScoredOnWithConstantScoreQuery(short stockValue, float boostValue, String explanation) throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) 100),
                    new ProductDocument(2, "Keyboard", (short) 5000)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a constant score query
            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
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
                        (short) 0, (short) 5000, 3L, List.of("1", "2", "5"),
                        "Range [0,5000] matches: Mouse(id=1,stock=100), Keyboard(id=2,stock=5000), Headset(id=5,stock=0)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        (short) 100, (short) 100, 1L, List.of("1"),
                        "Range [100,100] matches exact value: Mouse(id=1,stock=100)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        (short) 5001, (short) 32767, 1L, List.of("3"),
                        "Range [5001,32767] matches: Monitor(id=3,stock=10000)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        (short) -32768, (short) 0, 2L, List.of("4", "5"),
                        "Range [-32768,0] matches: Cable(id=4,stock=-5000), Headset(id=5,stock=0)"
                )
        );
    }

    /**
     * This test verifies that short fields support range queries for numeric comparisons.
     * The test documents are:
     * - id=1: Mouse, stock=100
     * - id=2: Keyboard, stock=5000
     * - id=3: Monitor, stock=10000
     * - id=4: Cable, stock=-5000
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
    public void shortMapping_SupportsRangeQueries(short gte, short lte, long expectedHits, List<String> expectedDocIds, String explanation) throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) 100),
                    new ProductDocument(2, "Keyboard", (short) 5000),
                    new ProductDocument(3, "Monitor", (short) 10000),
                    new ProductDocument(4, "Cable", (short) -5000),
                    new ProductDocument(5, "Headset", (short) 0)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a range query
            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
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
     * This test verifies that match queries on short fields work correctly.
     * Match queries are typically for full-text search, but they also work with numeric fields
     * by converting the query value to the appropriate numeric type.
     *
     * @param stockValue  The stock value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "100, 'Match query finds exact short value 100'",
            "5000, 'Match query finds exact short value 5000'"
    })
    public void shortMapping_WorksWithMatchQuery(short stockValue, String explanation) throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) 100),
                    new ProductDocument(2, "Keyboard", (short) 5000)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a match query
            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
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

