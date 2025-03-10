package KeywordDemo;

import KeywordDemo.Documents.ProductDocument;
import KeywordDemo.Documents.UserFavouriteProducts;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchRequestLogger;
import TestInfrastructure.OpenSearchTestIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.aggregations.*;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.query_dsl.MatchAllQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for keyword field aggregations in OpenSearch.
 * These tests demonstrate how keyword fields can be used for various aggregation types.
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class KeywordAggregationTests {
    private static final Logger logger = LogManager.getLogger(KeywordAggregationTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public KeywordAggregationTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that keyword fields can be used for terms aggregation.
     * 
     * This function is used to define a keyword mapping for the Name of a product.
     * OpenSearch documentation: https://opensearch.org/docs/2.0/opensearch/supported-field-types/keyword/
     * ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
     * 
     * Consider if you require eager global ordinals when using terms aggregations
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/eager-global-ordinals.html#_what_are_global_ordinals
     * 
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedForTermsAggregation() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "mouse", 1),
                    new ProductDocument(2, "mouse pad", 2),
                    new ProductDocument(3, "mouse", 3),
                    new ProductDocument(4, "mouse", 4),
                    new ProductDocument(5, "mouse pad", 5)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with terms aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("name")
                                    .size(10)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            StringTermsAggregate termsAgg = response.aggregations().get("product_counts").sterms();

            // Extract each term and its associated number of hits
            Map<String, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            StringTermsBucket::key,
                            StringTermsBucket::docCount
                    ));

            // Format the results for verification
            String formattedResults = bucketCounts.entrySet().stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results
            assertThat(formattedResults).isEqualTo("mouse:3, mouse pad:2");
        }
    }

    /**
     * This test verifies that keyword fields can be used for cardinality metric aggregation.
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedForMetricAggregation_Cardinality() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "mouse", 1),
                    new ProductDocument(2, "mouse pad", 2),
                    new ProductDocument(3, "mouse", 3),
                    new ProductDocument(4, "mouse", 4),
                    new ProductDocument(5, "mouse pad", 5)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with cardinality aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("distinctProductTypes", a -> a
                            .cardinality(c -> c
                                    .field("name")
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            CardinalityAggregate cardinalityAgg = response.aggregations().get("distinctProductTypes").cardinality();
            assertThat(cardinalityAgg.value()).isEqualTo(2);
        }
    }

    /**
     * This test verifies that keyword fields can be used for top hits aggregation.
     * Top hits is not recommended as a top level aggregation. <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-top-hits-aggregation.html">link</a>
     * Group using collapse instead {@link KeywordAggregationTests#keywordMapping_CanBeUsedForTermAggregation_Collapse}
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedForTermAggregation_TopHits() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "mouse", 1),
                    new ProductDocument(2, "mouse pad", 2),
                    new ProductDocument(3, "mouse", 3),
                    new ProductDocument(4, "mouse", 4),
                    new ProductDocument(5, "mouse pad", 5)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with terms aggregation and top hits sub-aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("productTypes", a -> a
                            .terms(t -> t
                                    .field("name")
                                    .size(10)
                            )
                            .aggregations("topType", sa -> sa
                                    .topHits(th -> th
                                            .size(1)
                                            .sort(s -> s.field(f -> f.field("rank").order(SortOrder.Desc)))
                                    )
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            StringTermsAggregate termsAgg = response.aggregations().get("productTypes").sterms();

            // Format the results for verification
            String formattedResults = termsAgg.buckets().array().stream()
                    .map(bucket -> {
                        TopHitsAggregate topHits = bucket.aggregations().get("topType").topHits();
                        var hit = topHits.hits().hits().get(0);
                        return hit.id() + ":" + bucket.key();
                    })
                    .collect(Collectors.joining(", "));

            // Verify the expected results
            assertThat(formattedResults).isEqualTo("4:mouse, 5:mouse pad");
        } catch (Exception e) {
            logger.error("failed to do thing");
        }
    }

    /**
     * This test verifies that keyword fields can be used for field collapsing.
     * Collapse the results on a given field, extracting the top result based on the sorting criteria specified.
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedForTermAggregation_Collapse() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "mouse", 1),
                    new ProductDocument(2, "mouse pad", 2),
                    new ProductDocument(3, "mouse", 3),
                    new ProductDocument(4, "mouse", 4),
                    new ProductDocument(5, "mouse pad", 5)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with field collapsing
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .query(query -> query.matchAll(new MatchAllQuery.Builder().build()))
                    .collapse(c -> c
                            .field("name")
                    )
                    .sort(s -> s.field(f -> f.field("rank").order(SortOrder.Desc)))
                    .from(0)
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.hits().hits()).hasSize(2);

            // Format the results for verification
            String formattedResults = response.hits().hits().stream()
                    .map(hit -> hit.id() + ":" + hit.source().getName())
                    .collect(Collectors.joining(", "));

            // Verify the expected results
            assertThat(formattedResults).isEqualTo("5:mouse pad, 4:mouse");
        }
    }

    /**
     * Per: <a href="https://github.com/opensearch-project/opensearch-java/issues/1478">this GitHub bug that I have raised</a>
     * The adjacency matrix is unusable in the Java client until a key field is added to the AdjacencyMatrixBucket
     * <p>
     * This test verifies that keyword fields can be used for adjacency matrix aggregation.
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedForAdjacencyMatrixAggregation() throws Exception {
        // Create a test index with keyword mapping for the ProductNames field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("productNames", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index user favourite products documents
            UserFavouriteProducts[] userFavouriteProducts = new UserFavouriteProducts[]{
                    new UserFavouriteProducts(1, new String[]{"mouse", "mouse pad"}),
                    new UserFavouriteProducts(2, new String[]{"mouse"}),
                    new UserFavouriteProducts(3, new String[]{"keyboard"}),
                    new UserFavouriteProducts(4, new String[]{"mouse pad", "keyboard"}),
                    new UserFavouriteProducts(5, new String[]{"mouse", "keyboard"}),
                    new UserFavouriteProducts(6, new String[]{"mouse", "mouse pad"})
            };
            testIndex.indexDocuments(userFavouriteProducts);

            // Create a map of the different filters
            var filterMap = Map.of(
                    "mouse", Query.of(q -> q.term(t -> t.field("productNames").value(FieldValue.of("mouse")))),
                    "mouse pad", Query.of(q -> q.term(t -> t.field("productNames").value(FieldValue.of("mouse pad")))),
                    "keyboard", Query.of(q -> q.term(t -> t.field("productNames").value(FieldValue.of("keyboard")))));

            // Create a search request with adjacency matrix aggregation
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .query(builder -> builder.matchAll(new MatchAllQuery.Builder().build()))
                    .aggregations("product_matrix", a -> a
                            .adjacencyMatrix(am -> am
                                    .filters(filterMap)
                            )
                    )
                    .build();

            OpenSearchRequestLogger.LogRequestJson(searchRequest);

            // Execute the search request
            SearchResponse<UserFavouriteProducts> response = openSearchClient.search(searchRequest, UserFavouriteProducts.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            AdjacencyMatrixAggregate matrixAgg = response.aggregations().get("product_matrix").adjacencyMatrix();

//            // Check the bucket counts
//            Map<String, Long> bucketCounts = matrixAgg.buckets().array().stream()
//                    .collect(Collectors.toMap(
//                            // This field is missing
//                            AdjacencyMatrixBucket::key,
//                            AdjacencyMatrixBucket::docCount
//                    ));
//
//            // Format the results for verification
//            String formattedResults = bucketCounts.entrySet().stream()
//                    .map(entry -> entry.getKey() + ":" + entry.getValue())
//                    .collect(Collectors.joining(", "));
//
//            // Verify the expected results
//            assertThat(bucketCounts).containsEntry("keyboard", 3L);
//            assertThat(bucketCounts).containsEntry("keyboard&mouse", 1L);
//            assertThat(bucketCounts).containsEntry("keyboard&mouse pad", 1L);
//            assertThat(bucketCounts).containsEntry("mouse", 4L);
//            assertThat(bucketCounts).containsEntry("mouse pad", 3L);
//            assertThat(bucketCounts).containsEntry("mouse&mouse pad", 2L);
        }
    }
}