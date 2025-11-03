package KeywordDemo;

import KeywordDemo.Documents.ProductDocument;
import KeywordDemo.Documents.ProductDocumentWithMultipleNames;
import KeywordDemo.Documents.UserFavouriteProducts;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for keyword field aggregations in OpenSearch.
 * These tests demonstrate how keyword fields can be used for various aggregation types.
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class KeywordAggregationTests {
    private static final Logger logger = LogManager.getLogger(KeywordAggregationTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public KeywordAggregationTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that keyword fields can be used for terms aggregation.
     * <p>
     * This function is used to define a keyword mapping for the Name of a product.
     * OpenSearch documentation: https://opensearch.org/docs/2.0/opensearch/supported-field-types/keyword/
     * ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
     * <p>
     * Consider if you require eager global ordinals when using terms aggregations
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/eager-global-ordinals.html#_what_are_global_ordinals
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedForTermsAggregationOnSingleKeyword() throws Exception {
        String testMethodName = "keywordMapping_CanBeUsedForTermsAggregationOnSingleKeyword";
        
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

            // Execute the search request with terms aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("name")
                                    .size(10)
                            )
                    ),
                    ProductDocument.class);

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
     * This test verifies that keyword fields with a normalizer can be used for terms aggregation.
     * <p>
     * Using a lowercase normalizer with a keyword field provides case-insensitive exact matching
     * without the tokenization behavior of text fields. This is more efficient than using
     * a text field with a custom analyzer when you need exact matching with case normalization.
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedForTermsAggregationOnSingleKeywordWithNormalizer() throws Exception {
        String testMethodName = "keywordMapping_CanBeUsedForTermsAggregationOnSingleKeywordWithNormalizer";
        
        // Create a test index with keyword mapping and a lowercase normalizer for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(
                // First parameter: Mapping configuration
                mapping -> mapping.properties("name", Property.of(p -> p.keyword(k -> k
                        .normalizer("lowercase_normalizer")
                        .norms(false)
                ))),
                // Second parameter: Settings configuration with the normalizer
                settings -> settings.analysis(a -> a
                        .normalizer("lowercase_normalizer", normalizer -> normalizer
                                .custom(c -> c
                                        .filter("lowercase")
                                )
                        )
                )
        )) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "Mouse", 1),  // Capital M to test lowercase normalizer
                    new ProductDocument(2, "mouse pad", 2),
                    new ProductDocument(3, "MOUSE", 3),  // All caps to test lowercase normalizer
                    new ProductDocument(4, "mouse", 4),
                    new ProductDocument(5, "Mouse Pad", 5)  // Mixed case to test lowercase normalizer
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with terms aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("name")
                                    .size(10)
                            )
                    ),
                    ProductDocument.class);

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
            // Note: All terms should be lowercase due to the normalizer regardless of original case
            assertThat(formattedResults).isEqualTo("mouse:3, mouse pad:2");
        }
    }

    /**
     * This test verifies that arrays of keyword fields can be used for terms aggregation.
     * <p>
     * This function demonstrates how to define a keyword mapping for an array of product names
     * and how to perform a terms aggregation on that field.
     * <p>
     * Consider if you require eager global ordinals when using terms aggregations
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/eager-global-ordinals.html#_what_are_global_ordinals
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedForTermsAggregationOnKeywordArray() throws Exception {
        // Create a test index with keyword mapping for the names array field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("names", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents with array of names
            ProductDocumentWithMultipleNames[] productDocuments = new ProductDocumentWithMultipleNames[]{
                    new ProductDocumentWithMultipleNames(1, new String[]{"mouse", "computer"}, 1),
                    new ProductDocumentWithMultipleNames(2, new String[]{"mouse pad", "power cable"}, 2),
                    new ProductDocumentWithMultipleNames(3, new String[]{"mouse", "mouse pad"}, 3),
                    new ProductDocumentWithMultipleNames(4, new String[]{"mouse", "arm rest pad"}, 4),
                    new ProductDocumentWithMultipleNames(5, new String[]{"mouse pad"}, 5)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with terms aggregation
            SearchResponse<ProductDocumentWithMultipleNames> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("names")
                                    .size(10)
                            )
                    ),
                    ProductDocumentWithMultipleNames.class);

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

            // Verify the expected results - mouse appears in 3 documents, mouse pad in 3, etc.
            assertThat(formattedResults).isEqualTo("mouse:3, computer:1, mouse pad:3, power cable:1, arm rest pad:1");
        }
    }

    /**
     * This test verifies that terms aggregations on keyword arrays count all terms in matching documents,
     * even when the documents are filtered by a query.
     * <p>
     * When a document matches a query, all terms in its arrays are counted in the aggregation,
     * not just the terms that matched the query.
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_TermsAggregationOnKeywordArrayCountsAllTermsWhenFiltered() throws Exception {
        // Create a test index with keyword mapping for the names array field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("names", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents with array of names
            ProductDocumentWithMultipleNames[] productDocuments = new ProductDocumentWithMultipleNames[]{
                    new ProductDocumentWithMultipleNames(1, new String[]{"mouse", "computer"}, 1),
                    new ProductDocumentWithMultipleNames(2, new String[]{"mouse pad", "power cable"}, 2),
                    new ProductDocumentWithMultipleNames(3, new String[]{"mouse", "mouse pad"}, 3),
                    new ProductDocumentWithMultipleNames(4, new String[]{"mouse", "arm rest pad"}, 4),
                    new ProductDocumentWithMultipleNames(5, new String[]{"mouse pad"}, 5)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with a term query on "computer" and a terms aggregation
            // This test adds a query to reduce the overall applicable documents. Only 1 document will match and have its terms aggregated
            SearchResponse<ProductDocumentWithMultipleNames> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .query(q -> q
                            .term(t -> t
                                    .field("names")
                                    .value(FieldValue.of("computer"))
                            )
                    )
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("names")
                                    .size(10)
                            )
                    ),
                    ProductDocumentWithMultipleNames.class);

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            // Verify that the query matched only one document
            assertThat(response.hits().total().value()).isEqualTo(1);

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

            // Verify the expected results - both "mouse" and "computer" are counted in the one matching document.
            // Despite a single document being produced from the match term query, all of its values in the field are aggregated over
            assertThat(formattedResults).isEqualTo("mouse:1, computer:1");
        }
    }

    /**
     * This test verifies that keyword fields can be used for filtered terms aggregation.
     * It demonstrates how to use the 'includes' parameter with regex patterns to filter terms.
     *
     * @param includesPattern The regex pattern to include terms
     * @param expectedResults The expected aggregation results in "term:count" format
     * @param description     A description of what the test case is evaluating
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "mouse, mouse:3, 'Exact match - matches only the exact term'",
            "mouse.*, 'mouse:3, mouse pad:2', 'Prefix match - matches terms starting with mouse'",
            ".*pad, mouse pad:2, 'Suffix match - matches terms ending with pad'",
            "keyboard, '', 'No matches - pattern matches no terms'"
    })
    public void keywordMapping_CanBeUsedForFilteredTermsAggregation_OnSingleKeywordWithIncludeRegularExpression(String includesPattern, String expectedResults, String description) throws Exception {
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

            // Execute the search request with terms aggregation and includes filter using regexp
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("name")
                                    .size(10)
                                    .include(i -> i.regexp(includesPattern))
                            )
                    ),
                    ProductDocument.class);

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
            assertThat(formattedResults)
                    .as(description)
                    .isEqualTo(expectedResults);
        }
    }

    /**
     * This test verifies that keyword fields can be used for filtered terms aggregation.
     * It demonstrates how to use the 'includes' parameter with explicit term values to filter terms.
     * <p>
     * Unlike the regex version, this approach allows exact matching of specific terms without
     * the complexity of regular expressions.
     *
     * @param includeTerms    Array of terms to include in the aggregation
     * @param expectedResults The expected aggregation results in "term:count" format
     * @param description     A description of what the test case is evaluating
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @MethodSource("provideIncludeTermsTestData")
    public void keywordMapping_CanBeUsedForFilteredTermsAggregation_OnSingleKeywordWithIncludeTerms(
            String[] includeTerms,
            String expectedResults,
            String description) throws Exception {
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

            // Execute the search request with terms aggregation and includes filter using explicit terms
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("name")
                                    .size(10)
                                    .include(i -> i.terms(Arrays.asList(includeTerms)))
                            )
                    ),
                    ProductDocument.class);

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
            assertThat(formattedResults)
                    .as(description)
                    .isEqualTo(expectedResults);
        }
    }

    private static Stream<Arguments> provideIncludeTermsTestData() {
        return Stream.of(
                Arguments.of(new String[]{"mouse"}, "mouse:3", "Include only mouse - filters out mouse pad"),
                Arguments.of(new String[]{"mouse pad"}, "mouse pad:2", "Include only mouse pad - filters out mouse"),
                Arguments.of(new String[]{"mouse", "mouse pad"}, "mouse:3, mouse pad:2", "Include both terms - shows all terms"),
                Arguments.of(new String[]{"keyboard"}, "", "Include non-existent term - no results")
        );
    }

    /**
     * This test verifies that keyword fields can be used for filtered terms aggregation with exclusion patterns.
     * It demonstrates how to use the 'excludes' parameter to filter out terms based on a regex pattern.
     *
     * @param excludesPattern The regex pattern to exclude terms
     * @param expectedResults The expected aggregation results in "term:count" format
     * @param description     A description of what the test case is evaluating
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "mouse, mouse pad:2, 'Exact exclude - excludes exactly the term mouse'",
            "mouse.*, '', 'Prefix exclude - excludes terms starting with mouse'",
            ".*pad, mouse:3, 'Suffix exclude - excludes terms ending with pad'"
    })
    public void keywordMapping_CanBeUsedForFilteredTermsAggregation_OnSingleKeywordWithExcludeRegularExpression(String excludesPattern, String expectedResults, String description) throws Exception {
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

            // Execute the search request with terms aggregation and excludes filter using regexp
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("name")
                                    .size(10)
                                    .exclude(e -> e.regexp(excludesPattern))
                            )
                    ),
                    ProductDocument.class);

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
            assertThat(formattedResults)
                    .as(description)
                    .isEqualTo(expectedResults);
        }
    }

    /**
     * This test verifies that keyword fields can be used for filtered terms aggregation with exclusion by specific terms.
     * It demonstrates how to use the 'excludes' parameter with explicit term values to filter out terms.
     * <p>
     * Unlike the regex version, this approach allows exact exclusion of specific terms without
     * the complexity of regular expressions.
     *
     * @param excludeTerms    Array of terms to exclude from the aggregation
     * @param expectedResults The expected aggregation results in "term:count" format
     * @param description     A description of what the test case is evaluating
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @MethodSource("provideExcludeTermsTestData")
    public void keywordMapping_CanBeUsedForFilteredTermsAggregation_OnSingleKeywordWithExcludeTerms(
            String[] excludeTerms,
            String expectedResults,
            String description) throws Exception {
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

            // Execute the search request with terms aggregation and excludes filter using explicit terms
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("name")
                                    .size(10)
                                    .exclude(e -> e.terms(Arrays.asList(excludeTerms)))
                            )
                    ),
                    ProductDocument.class);

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
            assertThat(formattedResults)
                    .as(description)
                    .isEqualTo(expectedResults);
        }
    }

    private static Stream<Arguments> provideExcludeTermsTestData() {
        return Stream.of(
                Arguments.of(new String[]{"mouse pad"}, "mouse:3", "Exclude mouse pad - only mouse remains"),
                Arguments.of(new String[]{"mouse"}, "mouse pad:2", "Exclude mouse - only mouse pad remains"),
                Arguments.of(new String[]{"mouse", "mouse pad"}, "", "Exclude both terms - no results")
        );
    }

    /**
     * This test verifies that arrays of keyword fields can be used for filtered terms aggregation.
     * It demonstrates how to use the 'includes' parameter with regex patterns to filter terms.
     * <p>
     * See {@link KeywordAggregationTests#keywordMapping_CanBeUsedForTermsAggregationOnKeywordArray} for the base case
     * that counts all elements across all document arrays without filtering.
     * <p>
     * See {@link KeywordAggregationTests#keywordMapping_TermsAggregationOnKeywordArrayCountsAllTermsWhenFiltered} for an example
     * of how all terms in an array are counted in the aggregation even when documents are filtered by a query on one of those terms.
     *
     * @param includesPattern The regex pattern to include terms
     * @param expectedResults The expected aggregation results in "term:count" format
     * @param description     A description of what the test case is evaluating
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "mouse, mouse:3, 'Exact match - matches only the exact term'",
            "mouse.*, 'mouse:3, mouse pad:3', 'Prefix match - matches terms starting with mouse'",
            ".*pad, 'mouse pad:3, arm rest pad:1', 'Suffix match - matches terms ending with pad'",
            "keyboard, '', 'No matches - pattern matches no terms'",
            "'.*', 'mouse:3, computer:1, mouse pad:3, power cable:1, arm rest pad:1', 'Match all - pattern matches all terms'"
    })
    public void keywordMapping_CanBeUsedForFilteredTermsAggregation_OnKeywordArrayWithIncludeRegularExpression(String includesPattern, String expectedResults, String description) throws Exception {
        // Create a test index with keyword mapping for the names array field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("names", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents with array of names
            ProductDocumentWithMultipleNames[] productDocuments = new ProductDocumentWithMultipleNames[]{
                    new ProductDocumentWithMultipleNames(1, new String[]{"mouse", "computer"}, 1),
                    new ProductDocumentWithMultipleNames(2, new String[]{"mouse pad", "power cable"}, 2),
                    new ProductDocumentWithMultipleNames(3, new String[]{"mouse", "mouse pad"}, 3),
                    new ProductDocumentWithMultipleNames(4, new String[]{"mouse", "arm rest pad"}, 4),
                    new ProductDocumentWithMultipleNames(5, new String[]{"mouse pad"}, 5)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with terms aggregation and includes filter using regexp
            SearchResponse<ProductDocumentWithMultipleNames> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("names")
                                    .size(10)
                                    .include(i -> i.regexp(includesPattern))
                            )
                    ),
                    ProductDocumentWithMultipleNames.class);

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
            assertThat(formattedResults)
                    .as(description)
                    .isEqualTo(expectedResults);
        }
    }

    /**
     * This test verifies that arrays of keyword fields can be used for filtered terms aggregation
     * using explicit term lists rather than regular expressions.
     * <p>
     * Unlike the regex version, this approach allows exact matching of specific terms without
     * the complexity of regular expressions.
     * <p>
     * See {@link KeywordAggregationTests#keywordMapping_CanBeUsedForTermsAggregationOnKeywordArray} for the base case
     * that counts all elements across all document arrays without filtering.
     *
     * @param includeTerms    Array of terms to include in the aggregation
     * @param expectedResults The expected aggregation results in "term:count" format
     * @param description     A description of what the test case is evaluating
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @MethodSource("provideIncludeTermsArrayTestData")
    public void keywordMapping_CanBeUsedForFilteredTermsAggregation_OnKeywordArrayWithIncludeTerms(
            String[] includeTerms,
            String expectedResults,
            String description) throws Exception {
        // Create a test index with keyword mapping for the names array field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("names", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents with array of names
            ProductDocumentWithMultipleNames[] productDocuments = new ProductDocumentWithMultipleNames[]{
                    new ProductDocumentWithMultipleNames(1, new String[]{"mouse", "computer"}, 1),
                    new ProductDocumentWithMultipleNames(2, new String[]{"mouse pad", "power cable"}, 2),
                    new ProductDocumentWithMultipleNames(3, new String[]{"mouse", "mouse pad"}, 3),
                    new ProductDocumentWithMultipleNames(4, new String[]{"mouse", "arm rest pad"}, 4),
                    new ProductDocumentWithMultipleNames(5, new String[]{"mouse pad"}, 5)
            };
            testIndex.indexDocuments(productDocuments);

            // The includeTerms is now directly a String array, no need for parsing

            // Execute the search request with terms aggregation and includes filter using explicit terms
            SearchResponse<ProductDocumentWithMultipleNames> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("names")
                                    .size(10)
                                    .include(i -> i.terms(Arrays.asList(includeTerms)))
                            )
                    ),
                    ProductDocumentWithMultipleNames.class);

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
            assertThat(formattedResults)
                    .as(description)
                    .isEqualTo(expectedResults);
        }
    }

    private static Stream<Arguments> provideIncludeTermsArrayTestData() {
        return Stream.of(
                Arguments.of(new String[]{"mouse"}, "mouse:3", "Include only mouse - matches exact term"),
                Arguments.of(new String[]{"mouse pad"}, "mouse pad:3", "Include only mouse pad - matches exact term"),
                Arguments.of(new String[]{"mouse", "mouse pad"}, "mouse:3, mouse pad:3", "Include mouse terms - matches both terms"),
                Arguments.of(new String[]{"mouse", "computer"}, "mouse:3, computer:1", "Include mixed terms - matches one common and one rare term"),
                Arguments.of(new String[]{"keyboard"}, "", "Include non-existent term - no results")
        );
    }

    /**
     * This test verifies that arrays of keyword fields can be used for filtered terms aggregation with exclusion patterns.
     * It demonstrates how to use the 'excludes' parameter to filter out terms based on a regex pattern.
     * <p>
     * This test complements {@link KeywordAggregationTests#keywordMapping_CanBeUsedForFilteredTermsAggregation_OnKeywordArrayWithIncludeRegularExpression}
     * by showing how to exclude terms rather than include them.
     *
     * @param excludesPattern The regex pattern to exclude terms
     * @param expectedResults The expected aggregation results in "term:count" format
     * @param description     A description of what the test case is evaluating
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "mouse, 'computer:1, mouse pad:3, power cable:1, arm rest pad:1', 'Exact exclude - excludes exactly the term mouse'",
            "mouse.*, 'computer:1, power cable:1, arm rest pad:1', 'Prefix exclude - excludes terms starting with mouse'",
            ".*pad, 'mouse:3, computer:1, power cable:1', 'Suffix exclude - excludes terms ending with pad'",
            "'.*ouse.*|.*pad.*|.*ower.*|.*omputer.*|.*rest.*', '', 'Exclude all patterns - no results'"
    })
    public void keywordMapping_CanBeUsedForFilteredTermsAggregation_OnKeywordArrayWithExcludeRegularExpression(String excludesPattern, String expectedResults, String description) throws Exception {
        // Create a test index with keyword mapping for the names array field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("names", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents with array of names
            ProductDocumentWithMultipleNames[] productDocuments = new ProductDocumentWithMultipleNames[]{
                    new ProductDocumentWithMultipleNames(1, new String[]{"mouse", "computer"}, 1),
                    new ProductDocumentWithMultipleNames(2, new String[]{"mouse pad", "power cable"}, 2),
                    new ProductDocumentWithMultipleNames(3, new String[]{"mouse", "mouse pad"}, 3),
                    new ProductDocumentWithMultipleNames(4, new String[]{"mouse", "arm rest pad"}, 4),
                    new ProductDocumentWithMultipleNames(5, new String[]{"mouse pad"}, 5)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with terms aggregation and excludes filter using regexp
            SearchResponse<ProductDocumentWithMultipleNames> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("names")
                                    .size(10)
                                    .exclude(e -> e.regexp(excludesPattern))
                            )
                    ),
                    ProductDocumentWithMultipleNames.class);

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
            assertThat(formattedResults)
                    .as(description)
                    .isEqualTo(expectedResults);
        }
    }

    /**
     * This test verifies that arrays of keyword fields can be used for filtered terms aggregation
     * with exclusion by specific terms.
     * <p>
     * Unlike the regex version, this approach allows exact exclusion of specific terms without
     * the complexity of regular expressions.
     * <p>
     * This test complements {@link KeywordAggregationTests#keywordMapping_CanBeUsedForFilteredTermsAggregation_OnKeywordArrayWithIncludeTerms}
     * by showing how to exclude terms rather than include them.
     *
     * @param excludeTerms    Array of terms to exclude from the aggregation
     * @param expectedResults The expected aggregation results in "term:count" format
     * @param description     A description of what the test case is evaluating
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @MethodSource("provideExcludeTermsArrayTestData")
    public void keywordMapping_CanBeUsedForFilteredTermsAggregation_OnKeywordArrayWithExcludeTerms(
            String[] excludeTerms,
            String expectedResults,
            String description) throws Exception {
        // Create a test index with keyword mapping for the names array field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("names", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents with array of names
            ProductDocumentWithMultipleNames[] productDocuments = new ProductDocumentWithMultipleNames[]{
                    new ProductDocumentWithMultipleNames(1, new String[]{"mouse", "computer"}, 1),
                    new ProductDocumentWithMultipleNames(2, new String[]{"mouse pad", "power cable"}, 2),
                    new ProductDocumentWithMultipleNames(3, new String[]{"mouse", "mouse pad"}, 3),
                    new ProductDocumentWithMultipleNames(4, new String[]{"mouse", "arm rest pad"}, 4),
                    new ProductDocumentWithMultipleNames(5, new String[]{"mouse pad"}, 5)
            };
            testIndex.indexDocuments(productDocuments);

            // Execute the search request with terms aggregation and excludes filter using explicit terms
            SearchResponse<ProductDocumentWithMultipleNames> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("product_counts", a -> a
                            .terms(t -> t
                                    .field("names")
                                    .size(10)
                                    .exclude(e -> e.terms(Arrays.asList(excludeTerms)))
                            )
                    ),
                    ProductDocumentWithMultipleNames.class);

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
            assertThat(formattedResults)
                    .as(description)
                    .isEqualTo(expectedResults);
        }
    }

    private static Stream<Arguments> provideExcludeTermsArrayTestData() {
        return Stream.of(
                Arguments.of(new String[]{"mouse"}, "computer:1, mouse pad:3, power cable:1, arm rest pad:1", "Exclude mouse - returns all other terms"),
                Arguments.of(new String[]{"mouse", "mouse pad"}, "computer:1, power cable:1, arm rest pad:1", "Exclude mouse terms - returns remaining terms"),
                Arguments.of(new String[]{"mouse", "computer", "mouse pad", "power cable", "arm rest pad"}, "", "Exclude all terms - no results"),
                Arguments.of(new String[]{"keyboard"}, "mouse:3, computer:1, mouse pad:3, power cable:1, arm rest pad:1", "Exclude non-existent term - all terms appear")
        );
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

            // Execute the search request with cardinality aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .size(0) // We do not want any documents returned; just the aggregations
                    .aggregations("distinctProductTypes", a -> a
                            .cardinality(c -> c
                                    .field("name")
                            )
                    ),
                    ProductDocument.class);

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

            // Execute the search request with terms aggregation and nested top hits aggregation
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
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
                                            .sort(sort -> sort.field(f -> f.field("rank").order(SortOrder.Desc)))
                                    )
                            )
                    ),
                    ProductDocument.class);

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

            // Execute the search request with field collapsing
            SearchResponse<ProductDocument> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .query(query -> query.matchAll(new MatchAllQuery.Builder().build()))
                    .collapse(c -> c
                            .field("name")
                    )
                    .sort(sort -> sort.field(f -> f.field("rank").order(SortOrder.Desc)))
                    .from(0),
                    ProductDocument.class);

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
     * Per: <a href="https://github.com/opensearch-project/opensearch-java/issues/1478">GitHub issue #1478</a>
     * The adjacency matrix aggregation bug was fixed in OpenSearch Java client v3.0.0 - AdjacencyMatrixBucket now has a key() method.
     * <p>
     * This test verifies that keyword fields can be used for adjacency matrix aggregation.
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedForAdjacencyMatrixAggregation() throws Exception {
        String testMethodName = "keywordMapping_CanBeUsedForAdjacencyMatrixAggregation";
        
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

            // Execute the search request with automatic logging
            SearchResponse<UserFavouriteProducts> response = loggingOpenSearchClient.search(s -> s
                    .index(testIndex.getName())
                    .query(builder -> builder.matchAll(new MatchAllQuery.Builder().build()))
                    .aggregations("product_matrix", a -> a
                            .adjacencyMatrix(am -> am
                                    .filters(filterMap)
                            )
                    ),
                    UserFavouriteProducts.class
            );

            // Verify the results
            assertThat(response.aggregations()).isNotNull();

            AdjacencyMatrixAggregate matrixAgg = response.aggregations().get("product_matrix").adjacencyMatrix();

            // Bug fixed in OpenSearch Java client v3.0.0 - AdjacencyMatrixBucket now has a key() method
            // See: https://github.com/opensearch-project/opensearch-java/issues/1478
            assertThat(matrixAgg.buckets()).isNotNull();
            
            // Check the bucket counts
            Map<String, Long> bucketCounts = matrixAgg.buckets().array().stream()
                    .collect(Collectors.toMap(
                            AdjacencyMatrixBucket::key,
                            AdjacencyMatrixBucket::docCount
                    ));
            
            // Format the results for verification
            String formattedResults = bucketCounts.entrySet().stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(", "));

            // Verify the expected results
            assertThat(bucketCounts).containsEntry("keyboard", 3L);
            assertThat(bucketCounts).containsEntry("keyboard&mouse", 1L);
            assertThat(bucketCounts).containsEntry("keyboard&mouse pad", 1L);
            assertThat(bucketCounts).containsEntry("mouse", 4L);
            assertThat(bucketCounts).containsEntry("mouse pad", 3L);
            assertThat(bucketCounts).containsEntry("mouse&mouse pad", 2L);
        }
    }
}