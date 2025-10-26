package ByteDemo;

import ByteDemo.Documents.ProductDocument;
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
 * Tests for byte field searching in OpenSearch.
 * These tests demonstrate how byte fields can be queried using various query types.
 *
 * Byte fields support term-level queries and range queries suitable for numeric data.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class ByteSearchingTests {
    private static final Logger logger = LogManager.getLogger(ByteSearchingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public ByteSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that term queries on byte fields match exact numeric values.
     *
     * @param stockValue  The stock value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "10, 'Matches documents with exact stock value of 10'",
            "50, 'Matches documents with exact stock value of 50'",
            "127, 'Matches documents with maximum byte value (127)'",
            "-128, 'Matches documents with minimum byte value (-128)'"
    })
    public void byteMapping_ExactlyMatchesTermQuery(byte stockValue, String explanation) throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) 10),
                    new ProductDocument(2, "Keyboard", (byte) 50),
                    new ProductDocument(3, "Monitor", (byte) 127),
                    new ProductDocument(4, "Cable", (byte) -128)
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
     * This test verifies that byte fields can be filtered using boolean queries.
     *
     * @param stockValue  The stock value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "10, 'Boolean filter matches stock value of 10'",
            "50, 'Boolean filter matches stock value of 50'"
    })
    public void byteMapping_CanBeFilteredOnWithBooleanQuery(byte stockValue, String explanation) throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) 10),
                    new ProductDocument(2, "Keyboard", (byte) 50)
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
     * This test verifies that byte fields can be queried using constant score queries.
     *
     * @param stockValue  The stock value to search for
     * @param boostValue  The boost value to apply
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "10, 2.0, 'Constant score query with boost 2.0'",
            "50, 5.0, 'Constant score query with boost 5.0'"
    })
    public void byteMapping_CanBeFilteredAndScoredOnWithConstantScoreQuery(byte stockValue, float boostValue, String explanation) throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) 10),
                    new ProductDocument(2, "Keyboard", (byte) 50)
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
                        (byte) 0, (byte) 50, 3L, List.of("1", "2", "5"),
                        "Range [0,50] matches: Mouse(id=1,stock=10), Keyboard(id=2,stock=50), Headset(id=5,stock=0)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        (byte) 10, (byte) 10, 1L, List.of("1"),
                        "Range [10,10] matches exact value: Mouse(id=1,stock=10)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        (byte) 51, (byte) 127, 1L, List.of("3"),
                        "Range [51,127] matches: Monitor(id=3,stock=100)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        (byte) -128, (byte) 0, 2L, List.of("4", "5"),
                        "Range [-128,0] matches: Cable(id=4,stock=-50), Headset(id=5,stock=0)"
                )
        );
    }

    /**
     * This test verifies that byte fields support range queries for numeric comparisons.
     * The test documents are:
     * - id=1: Mouse, stock=10
     * - id=2: Keyboard, stock=50
     * - id=3: Monitor, stock=100
     * - id=4: Cable, stock=-50
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
    public void byteMapping_SupportsRangeQueries(byte gte, byte lte, long expectedHits, List<String> expectedDocIds, String explanation) throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) 10),
                    new ProductDocument(2, "Keyboard", (byte) 50),
                    new ProductDocument(3, "Monitor", (byte) 100),
                    new ProductDocument(4, "Cable", (byte) -50),
                    new ProductDocument(5, "Headset", (byte) 0)
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
     * This test verifies that match queries on byte fields work correctly.
     * Match queries are typically for full-text search, but they also work with numeric fields
     * by converting the query value to the appropriate numeric type.
     *
     * @param stockValue  The stock value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "10, 'Match query finds exact byte value 10'",
            "50, 'Match query finds exact byte value 50'"
    })
    public void byteMapping_WorksWithMatchQuery(byte stockValue, String explanation) throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) 10),
                    new ProductDocument(2, "Keyboard", (byte) 50)
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
