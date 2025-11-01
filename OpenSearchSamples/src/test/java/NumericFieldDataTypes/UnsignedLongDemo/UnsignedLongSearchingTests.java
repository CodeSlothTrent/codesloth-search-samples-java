package NumericFieldDataTypes.UnsignedLongDemo;

import NumericFieldDataTypes.UnsignedLongDemo.Documents.ProductDocument;
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

/**
 * Tests for unsigned_long field searching in OpenSearch.
 * 
 * Note: unsigned_long field type was introduced in OpenSearch 2.4.0
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class UnsignedLongSearchingTests {
    private static final Logger logger = LogManager.getLogger(UnsignedLongSearchingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public UnsignedLongSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    @ParameterizedTest
    @CsvSource({
            "1000, 'Matches documents with exact stock value of 1000'",
            "5000, 'Matches documents with exact stock value of 5000'"
    })
    public void unsignedLongMapping_ExactlyMatchesTermQuery(long stockValue, String explanation) throws Exception {
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.unsignedLong(ul -> ul))))) {

            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 1000L),
                    new ProductDocument(2, "Keyboard", 5000L),
                    new ProductDocument(3, "Monitor", 10000L)
            };
            testIndex.indexDocuments(productDocuments);

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
                        0L, 5000L, 2L, List.of("1", "2"),
                        "Range [0,5000] matches: Mouse(id=1,stock=1000), Keyboard(id=2,stock=5000)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        1000L, 1000L, 1L, List.of("1"),
                        "Range [1000,1000] matches exact value: Mouse(id=1,stock=1000)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        5001L, Long.MAX_VALUE, 1L, List.of("3"),
                        "Range [5001," + Long.MAX_VALUE + "] matches: Monitor(id=3,stock=10000)"
                )
        );
    }

    /**
     * This test verifies that unsigned_long fields support range queries for numeric comparisons.
     * The test documents are:
     * - id=1: Mouse, stock=1000
     * - id=2: Keyboard, stock=5000
     * - id=3: Monitor, stock=10000
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
    public void unsignedLongMapping_SupportsRangeQueries(long gte, long lte, long expectedHits, List<String> expectedDocIds, String explanation) throws Exception {
        // Create a test index with unsigned_long mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.unsignedLong(ul -> ul))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 1000L),
                    new ProductDocument(2, "Keyboard", 5000L),
                    new ProductDocument(3, "Monitor", 10000L)
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

