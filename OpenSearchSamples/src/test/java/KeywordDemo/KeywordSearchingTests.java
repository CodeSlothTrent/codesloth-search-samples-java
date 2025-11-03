package KeywordDemo;

import KeywordDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.indices.AnalyzeResponse;
import org.opensearch.client.opensearch.indices.analyze.AnalyzeToken;
import org.opensearch.client.json.JsonData;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for keyword field searching in OpenSearch.
 * These tests demonstrate how keyword fields are searched in OpenSearch.
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class KeywordSearchingTests {
    private static final Logger logger = LogManager.getLogger(KeywordSearchingTests.class);
    
    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public KeywordSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that term queries on keyword fields match exactly the whole term.
     * 
     * This function is used to define a keyword mapping for the Name of a product.
     * OpenSearch documentation: https://opensearch.org/docs/2.0/opensearch/supported-field-types/keyword/
     * ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
     *
     * @param termText    The text to search for
     * @param explanation The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
        "'mouse', 'Only the document with name mouse will match'",
        "'mouse pad', 'Only the document with name mouse pad will match'"
    })
    public void keywordMapping_ExactlyMatchesWholeTermQuery(String termText, String explanation) throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping -> 
            mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {
            
            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[] {
                new ProductDocument(1, "mouse", 1),
                new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a term query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                .index(testIndex.getName())
                .query(q -> q
                    .term(t -> t
                        .field("name")
                        .value(FieldValue.of(termText))
                    )
                )
                .explain(true),
                ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getName()).as(explanation).isEqualTo(termText);
        }
    }

    /**
     * This test verifies that keyword fields can be filtered using boolean queries.
     *
     * @param termText    The text to search for
     * @param explanation The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
        "'mouse', 'Only the document with name mouse will match'",
        "'mouse pad', 'Only the document with name mouse pad will match'"
    })
    public void keywordMapping_CanBeFilteredOnWithBooleanQuery(String termText, String explanation) throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping -> 
            mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {
            
            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[] {
                new ProductDocument(1, "mouse", 1),
                new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a boolean query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                .index(testIndex.getName())
                .query(q -> q
                    .bool(b -> b
                        .filter(f -> f
                            .term(t -> t
                                .field("name")
                                .value(FieldValue.of(termText))
                            )
                        )
                    )
                )
                .explain(true),
                ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getName()).as(explanation).isEqualTo(termText);
        }
    }

    /**
     * This test verifies that keyword fields can be filtered and scored using constant score queries.
     *
     * @param termText    The text to search for
     * @param explanation The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
        "'mouse', 'Only the document with name mouse will match'",
        "'mouse pad', 'Only the document with name mouse pad will match'"
    })
    public void keywordMapping_CanBeFilteredAndScoredOnWithConstantScoreQuery(String termText, String explanation) throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping -> 
            mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {
            
            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[] {
                new ProductDocument(1, "mouse", 1),
                new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a constant score query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                .index(testIndex.getName())
                .query(q -> q
                    .constantScore(cs -> cs
                        .filter(f -> f
                            .term(t -> t
                                .field("name")
                                .value(FieldValue.of(termText))
                            )
                        )
                        .boost(3.0f)
                    )
                )
                .explain(true),
                ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getName()).as(explanation).isEqualTo(termText);
            assertThat(result.hits().hits().get(0).score()).isEqualTo(3.0f);
        }
    }

    /**
     * This test verifies that keyword fields do not undergo query-time analysis for match queries.
     *
     * @param matchText         The text to search for
     * @param expectedTokensStr The expected tokens as a comma-separated string
     * @param explanation       The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
        "'mouse', 'mouse', 'Only the document with name mouse will match'",
        "'mouse pad', 'mouse,pad', 'If the standard analyzer was run on this text it would produce two tokens: mouse, pad. Neither individual token would exactly match the mouse pad document name resulting in no document being returned. However, OpenSearch identifies that the mapping of the field is not Text and does not apply an analyzer at query time. This default behaviour only applies for text field mappings.'"
    })
    public void keywordMapping_ProducesNoQueryTimeAnalysis_ForMatchQuery(String matchText, String expectedTokensStr, String explanation) throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping -> 
            mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {
            
            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[] {
                new ProductDocument(1, "mouse", 1),
                new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a match query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                .index(testIndex.getName())
                .query(q -> q
                    .match(m -> m
                        .field("name")
                        .query(FieldValue.of(matchText))
                    )
                )
                .explain(true),
                ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits()).hasSize(1);
            assertThat(result.hits().hits().get(0).source().getName()).as(explanation).isEqualTo(matchText);

            // Analyze the text to confirm the tokens that would have been generated
            AnalyzeResponse analyzeResult = loggingOpenSearchClient.getClient().indices().analyze(a -> a
                .analyzer("standard")
                .text(matchText)
            );

            List<String> expectedTokensList = Arrays.asList(expectedTokensStr.split(","));
            List<String> actualTokens = analyzeResult.tokens().stream()
                .map(AnalyzeToken::token)
                .collect(Collectors.toList());
            
            assertThat(actualTokens).containsExactlyElementsOf(expectedTokensList);
        }
    }

    /**
     * This test verifies that keyword fields do not match on slightly mismatched terms.
     *
     * @param termText    The text to search for
     * @param explanation The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
        "'mous', 'Missing a letter'",
        "'mousepad', 'Missing a space'",
        "'Mouse pad', 'Missing a space'"
    })
    public void keywordMapping_DoesNotMatchOnSlightlyMismatchedTerms(String termText, String explanation) throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping -> 
            mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {
            
            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[] {
                new ProductDocument(1, "mouse", 1),
                new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a match query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                .index(testIndex.getName())
                .query(q -> q
                    .match(m -> m
                        .field("name")
                        .query(FieldValue.of(termText))
                    )
                ),
                ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value()).as(explanation).isEqualTo(0);
            assertThat(result.hits().hits()).as(explanation).isEmpty();
        }
    }

    /**
     * Test data provider for range query tests.
     * Each test case specifies the range boundaries (lexicographic), expected matching document IDs, and explanation.
     */
    private static Stream<org.junit.jupiter.params.provider.Arguments> rangeQueryTestCases() {
        return Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(
                        "a", "mouse", 1L, List.of("1"),
                        "Range [a to mouse] matches: Mouse(id=1,name=mouse) - lexicographic range"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        "mouse", "mouse", 1L, List.of("1"),
                        "Range [mouse,mouse] matches exact value: Mouse(id=1,name=mouse)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        "mouse pad", "z", 1L, List.of("2"),
                        "Range [mouse pad to z] matches: Keyboard(id=2,name=mouse pad)"
                ),
                org.junit.jupiter.params.provider.Arguments.of(
                        "a", "zzz", 2L, List.of("1", "2"),
                        "Range [a to zzz] matches all keyword values"
                )
        );
    }

    /**
     * This test verifies that keyword fields support range queries for lexicographic (string) comparisons.
     * Keyword fields support range queries based on lexicographic ordering.
     * The test documents are:
     * - id=1: Mouse, name="mouse"
     * - id=2: Keyboard, name="mouse pad"
     *
     * @param gte            Greater than or equal to value (lexicographic)
     * @param lte            Less than or equal to value (lexicographic)
     * @param expectedHits   Expected number of matching documents
     * @param expectedDocIds List of expected document IDs
     * @param explanation    The explanation of the test case
     * @throws Exception If an I/O error occurs
     */
    @ParameterizedTest
    @MethodSource("rangeQueryTestCases")
    public void keywordMapping_SupportsRangeQueries(String gte, String lte, long expectedHits, List<String> expectedDocIds, String explanation) throws Exception {
        // Create a test index with keyword mapping for the name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "mouse", 1),
                    new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Search for documents with a range query
            SearchResponse<ProductDocument> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .range(r -> r
                                            .field("name")
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