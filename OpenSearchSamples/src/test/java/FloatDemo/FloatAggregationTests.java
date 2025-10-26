package FloatDemo;

import FloatDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch._types.aggregations.DoubleTermsAggregate;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for float field aggregations in OpenSearch.
 * These tests demonstrate how float fields can be used for various aggregation types.
 *
 * Float fields support numeric aggregations including terms, cardinality, and other metric aggregations.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class FloatAggregationTests {
    private static final Logger logger = LogManager.getLogger(FloatAggregationTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public FloatAggregationTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that float fields can be used for terms aggregation.
     * Terms aggregation groups documents by distinct values in the float field.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void floatMapping_CanBeUsedForTermsAggregation() throws Exception {
        // Create a test index with float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.float_(f -> f))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f),
                    new ProductDocument(3, "Monitor", 19.99f),
                    new ProductDocument(4, "Cable", 19.99f),
                    new ProductDocument(5, "Headset", 49.95f)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with terms aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("price_counts", a -> a
                            .terms(t -> t
                                    .field("price")
                                    .size(10)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            DoubleTermsAggregate termsAgg = response.aggregations().get("price_counts").dterms();

            // Extract each term and its associated number of hits
            Map<Double, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> bucket.key(),
                            bucket -> bucket.docCount()
                    ));

            // Verify the expected bucket counts (using approximate comparison for doubles)
            assertThat(bucketCounts.size()).isEqualTo(2);
            assertThat(bucketCounts.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(5);
        }
    }

    /**
     * This test verifies that float fields can be used for cardinality metric aggregation.
     * Cardinality aggregation counts the number of distinct values in the float field.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void floatMapping_CanBeUsedForMetricAggregation_Cardinality() throws Exception {
        // Create a test index with float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.float_(f -> f))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f),
                    new ProductDocument(3, "Monitor", 19.99f),
                    new ProductDocument(4, "Cable", 19.99f),
                    new ProductDocument(5, "Headset", 49.95f)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with cardinality aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("distinctPrices", a -> a
                            .cardinality(c -> c
                                    .field("price")
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            CardinalityAggregate cardinalityAgg = response.aggregations().get("distinctPrices").cardinality();
            assertThat(cardinalityAgg.value()).isEqualTo(2);
        }
    }

    /**
     * This test verifies that float fields can be used for terms aggregation with filtering.
     * The aggregation only includes buckets where the price value is greater than a threshold.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void floatMapping_CanBeUsedForFilteredTermsAggregation() throws Exception {
        // Create a test index with float mapping for the price field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("price", Property.of(p -> p.float_(f -> f))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 19.99f),
                    new ProductDocument(2, "Keyboard", 49.95f),
                    new ProductDocument(3, "Monitor", 99.99f),
                    new ProductDocument(4, "Cable", 9.99f),
                    new ProductDocument(5, "Headset", 49.95f)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with a filter query and terms aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .query(q -> q
                            .range(r -> r
                                    .field("price")
                                    .gte(org.opensearch.client.json.JsonData.of(30.0f))
                            )
                    )
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("price_counts", a -> a
                            .terms(t -> t
                                    .field("price")
                                    .size(10)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(3); // Only 3 documents match the filter

            DoubleTermsAggregate termsAgg = response.aggregations().get("price_counts").dterms();

            // Verify bucket count
            assertThat(termsAgg.buckets().array()).hasSize(2); // 49.95 and 99.99
        }
    }
}

