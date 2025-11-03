package NumericFieldDataTypes.IntegerDemo;

import NumericFieldDataTypes.IntegerDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch._types.aggregations.CardinalityAggregate;
import org.opensearch.client.opensearch._types.aggregations.LongTermsAggregate;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for integer field aggregations in OpenSearch.
 * These tests demonstrate how integer fields can be used for various aggregation types.
 *
 * Integer fields support numeric aggregations including terms, cardinality, and other metric aggregations.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class IntegerAggregationTests {
    private static final Logger logger = LogManager.getLogger(IntegerAggregationTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public IntegerAggregationTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that integer fields can be used for terms aggregation.
     * Terms aggregation groups documents by distinct values in the integer field.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void integerMapping_CanBeUsedForTermsAggregation() throws Exception {
        // Create a test index with integer mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.integer(i -> i))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 1000),
                    new ProductDocument(2, "Keyboard", 500000),
                    new ProductDocument(3, "Monitor", 1000),
                    new ProductDocument(4, "Cable", 1000),
                    new ProductDocument(5, "Headset", 500000)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with terms aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("stock_counts", a -> a
                            .terms(t -> t
                                    .field("stock")
                                    .size(10)
                            )
                    ),
                    ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            LongTermsAggregate termsAgg = response.aggregations().get("stock_counts").lterms();

            // Extract each term and its associated number of hits
            Map<String, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> bucket.key().toString(),
                            bucket -> bucket.docCount()
                    ));

            // Format the results for verification (sorted by key for consistent ordering)
            String formattedResults = bucketCounts.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByKey(Comparator.comparing(Long::parseLong)))
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results
            assertThat(formattedResults).isEqualTo("1000:3, 500000:2");
        }
    }

    /**
     * This test verifies that integer fields can be used for cardinality metric aggregation.
     * Cardinality aggregation counts the number of distinct values in the integer field.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void integerMapping_CanBeUsedForMetricAggregation_Cardinality() throws Exception {
        // Create a test index with integer mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.integer(i -> i))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 1000),
                    new ProductDocument(2, "Keyboard", 500000),
                    new ProductDocument(3, "Monitor", 1000),
                    new ProductDocument(4, "Cable", 1000),
                    new ProductDocument(5, "Headset", 500000)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with cardinality aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("distinctStockLevels", a -> a
                            .cardinality(c -> c
                                    .field("stock")
                            )
                    ),
                    ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            CardinalityAggregate cardinalityAgg = response.aggregations().get("distinctStockLevels").cardinality();
            assertThat(cardinalityAgg.value()).isEqualTo(2);
        }
    }

    /**
     * This test verifies that integer fields can be used for terms aggregation with filtering.
     * The aggregation only includes buckets where the stock value is greater than a threshold.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void integerMapping_CanBeUsedForFilteredTermsAggregation() throws Exception {
        // Create a test index with integer mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.integer(i -> i))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 1000),
                    new ProductDocument(2, "Keyboard", 500000),
                    new ProductDocument(3, "Monitor", 1000000),
                    new ProductDocument(4, "Cable", 1000),
                    new ProductDocument(5, "Headset", 500000)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with a filter query and terms aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .query(q -> q
                            .range(r -> r
                                    .field("stock")
                                    .gte(org.opensearch.client.json.JsonData.of(100000))
                            )
                    )
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("stock_counts", a -> a
                            .terms(t -> t
                                    .field("stock")
                                    .size(10)
                            )
                    ),
                    ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(3); // Only 3 documents match the filter

            LongTermsAggregate termsAgg = response.aggregations().get("stock_counts").lterms();

            // Extract each term and its associated number of hits
            Map<String, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> bucket.key().toString(),
                            bucket -> bucket.docCount()
                    ));

            // Format the results for verification (sorted by key for consistent ordering)
            String formattedResults = bucketCounts.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByKey(Comparator.comparing(Long::parseLong)))
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results - only stock values >= 100000 should appear
            assertThat(formattedResults).isEqualTo("500000:2, 1000000:1");
        }
    }

    /**
     * This test verifies that integer fields can be used with terms aggregation to count
     * distinct values including negative numbers.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void integerMapping_TermsAggregationHandlesNegativeValues() throws Exception {
        // Create a test index with integer mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.integer(i -> i))))) {

            // Create and index product documents with negative and positive values
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", -100000),
                    new ProductDocument(2, "Keyboard", 500000),
                    new ProductDocument(3, "Monitor", -100000),
                    new ProductDocument(4, "Cable", 0),
                    new ProductDocument(5, "Headset", 500000)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with terms aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("stock_counts", a -> a
                            .terms(t -> t
                                    .field("stock")
                                    .size(10)
                            )
                    ),
                    ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            LongTermsAggregate termsAgg = response.aggregations().get("stock_counts").lterms();

            // Extract each term and its associated number of hits
            Map<String, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> bucket.key().toString(),
                            bucket -> bucket.docCount()
                    ));

            // Format the results for verification
            String formattedResults = bucketCounts.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByKey(Comparator.comparing(Long::parseLong)))
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results - should include negative values
            assertThat(formattedResults).isEqualTo("-100000:2, 0:1, 500000:2");
        }
    }

    /**
     * This test verifies that integer fields can be used with terms aggregation to handle
     * boundary values (min and max integer values).
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void integerMapping_TermsAggregationHandlesBoundaryValues() throws Exception {
        // Create a test index with integer mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.integer(i -> i))))) {

            // Create and index product documents with boundary values
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 2147483647),  // Max integer value
                    new ProductDocument(2, "Keyboard", -2147483648), // Min integer value
                    new ProductDocument(3, "Monitor", 2147483647),
                    new ProductDocument(4, "Cable", -2147483648),
                    new ProductDocument(5, "Headset", 0)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with terms aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("stock_counts", a -> a
                            .terms(t -> t
                                    .field("stock")
                                    .size(10)
                            )
                    ),
                    ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            LongTermsAggregate termsAgg = response.aggregations().get("stock_counts").lterms();

            // Extract each term and its associated number of hits
            Map<String, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> bucket.key().toString(),
                            bucket -> bucket.docCount()
                    ));

            // Verify the expected results - should correctly handle min and max integer values
            assertThat(bucketCounts.get(-2147483648L)).isEqualTo(2);
            assertThat(bucketCounts.get(0L)).isEqualTo(1);
            assertThat(bucketCounts.get(2147483647L)).isEqualTo(2);
        }
    }
}

