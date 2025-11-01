package NumericFieldDataTypes.HalfFloatDemo;

import NumericFieldDataTypes.HalfFloatDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.json.JsonData;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OpenSearchResourceManagementExtension.class)
public class HalfFloatSearchingTests {
    private static final Logger logger = LogManager.getLogger(HalfFloatSearchingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public HalfFloatSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @ParameterizedTest
    @CsvSource({
            "19.99, 'Matches documents with exact price value of 19.99'",
            "49.95, 'Matches documents with exact price value of 49.95'"
    })
    public void halfFloatMapping_ExactlyMatchesTermQuery(float priceValue, String explanation) throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.halfFloat(h -> h))))) {

            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f),
                    new ProductDocument(3, "Monitor", 99.99f)
            };
            testIndex.indexDocuments(productDocuments);

            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("price")
                                            .value(FieldValue.of(priceValue))
                                    )
                            ),
                    ProductDocument.class
            );

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
                        0.0f, 50.0f, 2L, List.of("1", "2"),
                        "Range [0.0,50.0] matches: Mouse(id=1,price=19.99), Keyboard(id=2,price=49.95)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        19.98f, 20.0f, 1L, List.of("1"),
                        "Range [19.98,20.0] matches: Mouse(id=1,price=19.99) - using range to avoid floating point precision issues"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        50.0f, 200.0f, 1L, List.of("3"),
                        "Range [50.0,200.0] matches: Monitor(id=3,price=99.99)"
                )
        );
    }

    /**
     * This test verifies that half_float fields support range queries for numeric comparisons.
     * The test documents are:
     * - id=1: Mouse, price=19.99
     * - id=2: Keyboard, price=49.95
     * - id=3: Monitor, price=99.99
     *
     * @param gte            Greater than or equal to value
     * @param lte            Less than or equal to value
     * @param expectedHits   Expected number of matching documents
     * @param expectedDocIds List of expected document IDs
     * @param explanation    The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @MethodSource("rangeQueryTestCases")
    public void halfFloatMapping_SupportsRangeQueries(float gte, float lte, long expectedHits, List<String> expectedDocIds, String explanation) throws Exception {
        // Create a test index with half_float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.halfFloat(h -> h))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f),
                    new ProductDocument(3, "Monitor", 99.99f)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a range query
            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
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
}

