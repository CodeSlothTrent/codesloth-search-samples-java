package NumericFieldDataTypes.ShortDemo;

import NumericFieldDataTypes.ShortDemo.Documents.ProductDocument;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.aggregations.CardinalityAggregate;
import org.opensearch.client.opensearch._types.aggregations.LongTermsAggregate;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for short field aggregations in OpenSearch.
 * These tests demonstrate how short fields can be used for various aggregation types.
 *
 * Short fields support numeric aggregations including terms, cardinality, and other metric aggregations.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class ShortAggregationTests {
    private static final Logger logger = LogManager.getLogger(ShortAggregationTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public ShortAggregationTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that short fields can be used for terms aggregation.
     * Terms aggregation groups documents by distinct values in the short field.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void shortMapping_CanBeUsedForTermsAggregation() throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) 100),
                    new ProductDocument(2, "Keyboard", (short) 5000),
                    new ProductDocument(3, "Monitor", (short) 100),
                    new ProductDocument(4, "Cable", (short) 100),
                    new ProductDocument(5, "Headset", (short) 5000)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with terms aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("stock_counts", a -> a
                            .terms(t -> t
                                    .field("stock")
                                    .size(10)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            LongTermsAggregate termsAgg = response.aggregations().get("stock_counts").lterms();

            // Extract each term and its associated number of hits
            Map<Long, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> Long.parseLong(bucket.key()),
                            bucket -> bucket.docCount()
                    ));

            // Format the results for verification (sorted by key for consistent ordering)
            String formattedResults = bucketCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results
            assertThat(formattedResults).isEqualTo("100:3, 5000:2");
        }
    }

    /**
     * This test verifies that short fields can be used for cardinality metric aggregation.
     * Cardinality aggregation counts the number of distinct values in the short field.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void shortMapping_CanBeUsedForMetricAggregation_Cardinality() throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) 100),
                    new ProductDocument(2, "Keyboard", (short) 5000),
                    new ProductDocument(3, "Monitor", (short) 100),
                    new ProductDocument(4, "Cable", (short) 100),
                    new ProductDocument(5, "Headset", (short) 5000)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with cardinality aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("distinctStockLevels", a -> a
                            .cardinality(c -> c
                                    .field("stock")
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            CardinalityAggregate cardinalityAgg = response.aggregations().get("distinctStockLevels").cardinality();
            assertThat(cardinalityAgg.value()).isEqualTo(2);
        }
    }

    /**
     * This test verifies that short fields can be used for terms aggregation with filtering.
     * The aggregation only includes buckets where the stock value is greater than a threshold.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void shortMapping_CanBeUsedForFilteredTermsAggregation() throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) 100),
                    new ProductDocument(2, "Keyboard", (short) 5000),
                    new ProductDocument(3, "Monitor", (short) 10000),
                    new ProductDocument(4, "Cable", (short) 100),
                    new ProductDocument(5, "Headset", (short) 5000)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with a filter query and terms aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .query(q -> q
                            .range(r -> r
                                    .field("stock")
                                    .gte(org.opensearch.client.json.JsonData.of(1000))
                            )
                    )
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("stock_counts", a -> a
                            .terms(t -> t
                                    .field("stock")
                                    .size(10)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(3); // Only 3 documents match the filter

            LongTermsAggregate termsAgg = response.aggregations().get("stock_counts").lterms();

            // Extract each term and its associated number of hits
            Map<Long, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> Long.parseLong(bucket.key()),
                            bucket -> bucket.docCount()
                    ));

            // Format the results for verification (sorted by key for consistent ordering)
            String formattedResults = bucketCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results - only stock values >= 1000 should appear
            assertThat(formattedResults).isEqualTo("5000:2, 10000:1");
        }
    }

    /**
     * This test verifies that short fields can be used with terms aggregation to count
     * distinct values including negative numbers.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void shortMapping_TermsAggregationHandlesNegativeValues() throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents with negative and positive values
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) -1000),
                    new ProductDocument(2, "Keyboard", (short) 5000),
                    new ProductDocument(3, "Monitor", (short) -1000),
                    new ProductDocument(4, "Cable", (short) 0),
                    new ProductDocument(5, "Headset", (short) 5000)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with terms aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("stock_counts", a -> a
                            .terms(t -> t
                                    .field("stock")
                                    .size(10)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            LongTermsAggregate termsAgg = response.aggregations().get("stock_counts").lterms();

            // Extract each term and its associated number of hits
            Map<Long, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> Long.parseLong(bucket.key()),
                            bucket -> bucket.docCount()
                    ));

            // Format the results for verification
            String formattedResults = bucketCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results - should include negative values
            assertThat(formattedResults).isEqualTo("-1000:2, 0:1, 5000:2");
        }
    }

    /**
     * This test verifies that short fields can be used with terms aggregation to handle
     * boundary values (min and max short values).
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void shortMapping_TermsAggregationHandlesBoundaryValues() throws Exception {
        // Create a test index with short mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.short_(s -> s))))) {

            // Create and index product documents with boundary values
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (short) 32767),  // Max short value
                    new ProductDocument(2, "Keyboard", (short) -32768), // Min short value
                    new ProductDocument(3, "Monitor", (short) 32767),
                    new ProductDocument(4, "Cable", (short) -32768),
                    new ProductDocument(5, "Headset", (short) 0)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with terms aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("stock_counts", a -> a
                            .terms(t -> t
                                    .field("stock")
                                    .size(10)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            LongTermsAggregate termsAgg = response.aggregations().get("stock_counts").lterms();

            // Extract each term and its associated number of hits
            Map<Long, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> Long.parseLong(bucket.key()),
                            bucket -> bucket.docCount()
                    ));

            // Verify the expected results - should correctly handle min and max short values
            assertThat(bucketCounts.get(-32768L)).isEqualTo(2);
            assertThat(bucketCounts.get(0L)).isEqualTo(1);
            assertThat(bucketCounts.get(32767L)).isEqualTo(2);
        }
    }
}

