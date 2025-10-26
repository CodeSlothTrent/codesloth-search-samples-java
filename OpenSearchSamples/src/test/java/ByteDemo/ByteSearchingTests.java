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
     * This test verifies that byte fields support range queries for numeric comparisons.
     *
     * @param gte         Greater than or equal to value
     * @param lte         Less than or equal to value
     * @param expectedHits Expected number of matching documents
     * @param explanation  The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "0, 50, 3, 'Range query matches values between 0 and 50'",
            "10, 10, 1, 'Range query with same min/max matches exact value'",
            "51, 127, 1, 'Range query matches values between 51 and 127'",
            "-128, 0, 2, 'Range query matches negative values and zero'"
    })
    public void byteMapping_SupportsRangeQueries(byte gte, byte lte, long expectedHits, String explanation) throws Exception {
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
