package NumericFieldDataTypes.ByteDemo;

import NumericFieldDataTypes.ByteDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch._types.aggregations.LongTermsBucket;
import org.opensearch.client.opensearch._types.aggregations.LongTermsBucketKey;
import org.opensearch.client.opensearch._types.aggregations.LongTermsAggregate;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for byte field aggregations in OpenSearch.
 * These tests demonstrate how byte fields can be used for various aggregation types.
 *
 * Byte fields support numeric aggregations including terms, cardinality, and other metric aggregations.
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/numeric/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class ByteAggregationTests {
    private static final Logger logger = LogManager.getLogger(ByteAggregationTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public ByteAggregationTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * Helper method to extract Long value from LongTermsBucketKey.
     * Workaround for incomplete API (similar to GitHub issue #1478).
     * Uses reflection to access the internal value field.
     */
    private Long extractLongFromKey(LongTermsBucketKey key) {
        try {
            // Try to access the value field via reflection
            // LongTermsBucketKey may be a record or have accessible fields
            java.lang.reflect.Field[] fields = key.getClass().getDeclaredFields();
            for (java.lang.reflect.Field field : fields) {
                field.setAccessible(true);
                Object value = field.get(key);
                if (value instanceof Long) {
                    return (Long) value;
                } else if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
            }
            
            // Try common field names
            String[] possibleFieldNames = {"value", "longValue", "_value", "key"};
            for (String fieldName : possibleFieldNames) {
                try {
                    java.lang.reflect.Field field = key.getClass().getDeclaredField(fieldName);
                    field.setAccessible(true);
                    Object value = field.get(key);
                    if (value instanceof Long) {
                        return (Long) value;
                    } else if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                } catch (NoSuchFieldException e) {
                    // Continue trying other fields
                }
            }
            
            // If reflection fails, try toString() parsing as fallback
            String keyStr = key.toString();
            if (!keyStr.contains("@")) {
                return Long.parseLong(keyStr);
            }
            
            throw new UnsupportedOperationException(
                "Unable to extract Long value from LongTermsBucketKey. " +
                "This may be unimplemented functionality (similar to GitHub issue #1478). " +
                "toString() returns: " + keyStr);
        } catch (IllegalAccessException | NumberFormatException e) {
            throw new RuntimeException("Failed to extract Long from LongTermsBucketKey", e);
        }
    }

    /**
     * This test verifies that byte fields can be used for terms aggregation.
     * Terms aggregation groups documents by distinct values in the byte field.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void byteMapping_CanBeUsedForTermsAggregation() throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) 10),
                    new ProductDocument(2, "Keyboard", (byte) 50),
                    new ProductDocument(3, "Monitor", (byte) 10),
                    new ProductDocument(4, "Cable", (byte) 10),
                    new ProductDocument(5, "Headset", (byte) 50)
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
            // LongTermsBucketKey API appears incomplete (similar to GitHub issue #1478)
            // Using workaround: parse the string representation to extract Long value
            Map<Long, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> extractLongFromKey(bucket.key()),
                            bucket -> bucket.docCount()
                    ));

            // Format the results for verification (sorted by key for consistent ordering)
            String formattedResults = bucketCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results
            assertThat(formattedResults).isEqualTo("10:3, 50:2");
        }
    }

    /**
     * This test verifies that byte fields can be used for cardinality metric aggregation.
     * Cardinality aggregation counts the number of distinct values in the byte field.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void byteMapping_CanBeUsedForMetricAggregation_Cardinality() throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) 10),
                    new ProductDocument(2, "Keyboard", (byte) 50),
                    new ProductDocument(3, "Monitor", (byte) 10),
                    new ProductDocument(4, "Cable", (byte) 10),
                    new ProductDocument(5, "Headset", (byte) 50)
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
     * This test verifies that byte fields can be used for terms aggregation with filtering.
     * The aggregation only includes buckets where the stock value is greater than a threshold.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void byteMapping_CanBeUsedForFilteredTermsAggregation() throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) 10),
                    new ProductDocument(2, "Keyboard", (byte) 50),
                    new ProductDocument(3, "Monitor", (byte) 100),
                    new ProductDocument(4, "Cable", (byte) 10),
                    new ProductDocument(5, "Headset", (byte) 50)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with a filter query and terms aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .query(q -> q
                            .range(r -> r
                                    .field("stock")
                                    .gte(org.opensearch.client.json.JsonData.of(30))
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
            Map<Long, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> extractLongFromKey(bucket.key()),
                            bucket -> bucket.docCount()
                    ));

            // Format the results for verification (sorted by key for consistent ordering)
            String formattedResults = bucketCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results - only stock values >= 30 should appear
            assertThat(formattedResults).isEqualTo("50:2, 100:1");
        }
    }

    /**
     * This test verifies that byte fields can be used with terms aggregation to count
     * distinct values including negative numbers.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void byteMapping_TermsAggregationHandlesNegativeValues() throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents with negative and positive values
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) -10),
                    new ProductDocument(2, "Keyboard", (byte) 50),
                    new ProductDocument(3, "Monitor", (byte) -10),
                    new ProductDocument(4, "Cable", (byte) 0),
                    new ProductDocument(5, "Headset", (byte) 50)
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
            Map<Long, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> extractLongFromKey(bucket.key()),
                            bucket -> bucket.docCount()
                    ));

            // Format the results for verification
            String formattedResults = bucketCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results - should include negative values
            assertThat(formattedResults).isEqualTo("-10:2, 0:1, 50:2");
        }
    }

    /**
     * This test verifies that byte fields can be used with terms aggregation to handle
     * boundary values (min and max byte values).
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void byteMapping_TermsAggregationHandlesBoundaryValues() throws Exception {
        // Create a test index with byte mapping for the stock field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("stock", Property.of(p -> p.byte_(b -> b))))) {

            // Create and index product documents with boundary values
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", (byte) 127),  // Max byte value
                    new ProductDocument(2, "Keyboard", (byte) -128), // Min byte value
                    new ProductDocument(3, "Monitor", (byte) 127),
                    new ProductDocument(4, "Cable", (byte) -128),
                    new ProductDocument(5, "Headset", (byte) 0)
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
            Map<Long, Long> bucketCounts = termsAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            bucket -> extractLongFromKey(bucket.key()),
                            bucket -> bucket.docCount()
                    ));

            // Verify the expected results - should correctly handle min and max byte values
            assertThat(bucketCounts.get(-128L)).isEqualTo(2);
            assertThat(bucketCounts.get(0L)).isEqualTo(1);
            assertThat(bucketCounts.get(127L)).isEqualTo(2);
        }
    }
}
