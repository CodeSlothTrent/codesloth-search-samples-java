package NumericFieldDataTypes.FloatDemo;

import NumericFieldDataTypes.FloatDemo.Documents.ProductDocument;
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
 * Tests for float field searching in OpenSearch.
 * These tests demonstrate how float fields can be queried using various query types.
 *
 * Float fields support term-level queries and range queries suitable for numeric data.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class FloatSearchingTests {
    private static final Logger logger = LogManager.getLogger(FloatSearchingTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public FloatSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that term queries on float fields match exact numeric values.
     *
     * @param priceValue  The price value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "19.99, 'Matches documents with exact price value of 19.99'",
            "49.95, 'Matches documents with exact price value of 49.95'",
            "99.99, 'Matches documents with exact price value of 99.99'",
            "0.99, 'Matches documents with exact price value of 0.99'"
    })
    public void floatMapping_ExactlyMatchesTermQuery(float priceValue, String explanation) throws Exception {
        // Create a test index with float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.float_(f -> f))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f),
                    new ProductDocument(3, "Monitor", 99.99f),
                    new ProductDocument(4, "Cable", 0.99f)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a term query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("price")
                                            .value(FieldValue.of(priceValue))
                                    )
                            ),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
        }
    }

    /**
     * This test verifies that float fields can be filtered using boolean queries.
     *
     * @param priceValue  The price value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "19.99, 'Boolean filter matches price value of 19.99'",
            "49.95, 'Boolean filter matches price value of 49.95'"
    })
    public void floatMapping_CanBeFilteredOnWithBooleanQuery(float priceValue, String explanation) throws Exception {
        // Create a test index with float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.float_(f -> f))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a boolean query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .bool(b -> b
                                            .filter(f -> f
                                                    .term(t -> t
                                                            .field("price")
                                                            .value(FieldValue.of(priceValue))
                                                    )
                                            )
                                    )
                            ),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
        }
    }

    /**
     * Test data provider for range query tests.
     * Each test case specifies the range boundaries, expected matching document IDs, and explanation.
     */
    private static Stream<org.junit.jupiter.params.provider.Arguments> rangeQueryTestCases() {
        return Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(
                        0.0f, 50.0f, 3L, List.of("1", "2", "4"),
                        "Range [0.0,50.0] matches: Mouse(id=1,price=19.99), Keyboard(id=2,price=49.95), Cable(id=4,price=0.99)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        19.99f, 19.99f, 1L, List.of("1"),
                        "Range [19.99,19.99] matches exact value: Mouse(id=1,price=19.99)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        50.0f, 200.0f, 1L, List.of("3"),
                        "Range [50.0,200.0] matches: Monitor(id=3,price=99.99)"
                )
        );
    }

    /**
     * This test verifies that float fields support range queries for numeric comparisons.
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
    public void floatMapping_SupportsRangeQueries(float gte, float lte, long expectedHits, List<String> expectedDocIds, String explanation) throws Exception {
        // Create a test index with float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.float_(f -> f))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f),
                    new ProductDocument(3, "Monitor", 99.99f),
                    new ProductDocument(4, "Cable", 0.99f)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a range query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("price")
                                            .gte(JsonData.of(gte))
                                            .lte(JsonData.of(lte))
                                    )
                            ),
                    ProductDocument.class
            );

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
     * This test verifies that match queries on float fields work correctly.
     *
     * @param priceValue  The price value to search for
     * @param explanation The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "19.99, 'Match query finds exact float value 19.99'",
            "49.95, 'Match query finds exact float value 49.95'"
    })
    public void floatMapping_WorksWithMatchQuery(float priceValue, String explanation) throws Exception {
        // Create a test index with float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.float_(f -> f))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a match query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .match(m -> m
                                            .field("price")
                                            .query(FieldValue.of(priceValue))
                                    )
                            ),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
        }
    }
}

