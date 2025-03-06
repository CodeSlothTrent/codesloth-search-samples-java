package TextDemo;

import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import TextDemo.Documents.ProductDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.analysis.Analyzer;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.TermvectorsResponse;
import org.opensearch.client.opensearch.core.termvectors.TermVector;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for text field analysis and searching in OpenSearch.
 * These tests demonstrate how text fields are analyzed and searched in OpenSearch.
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class TextTests {
    private static final Logger logger = LogManager.getLogger(TextTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public TextTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that text fields use the standard analyzer by default.
     * Additional reading about text analysis can be found here: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-overview.html
     * Additional reading about the standard analyzer can be found here: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-analyzer.html
     *
     * @param text              The text to index
     * @param expectedTokensCsv The expected tokens and frequencies
     * @param explanation       The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "'product', 'product:1', 'The standard analyzer does not produce additional tokens for single word strings'",
            "'great product', 'great:1, product:1', 'Two individual tokens are produced by the standard analyzer as it provides grammar based tokenisation'",
            "'This is a really amazing product. It is great, you absolutely must buy it!', 'a:1, absolutely:1, amazing:1, buy:1, great:1, is:2, it:2, must:1, product:1, really:1, this:1, you:1', 'Many tokens are produced. Grammar is stripped so that only words are indexed. Case is normalised to lowercase. Recurring words are counted against the same token.'"
    })
    public void textMapping_UsesStandardAnalyzerByDefault(String text, String expectedTokensCsv, String explanation) throws Exception {
        // Create a test index with text mapping for the Description field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("description", Property.of(p -> p.text(t -> t))))) {

            // Create and index a product document
            ProductDocument productDocument = new ProductDocument(1, text, 1);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Get term vectors for the document
            TermvectorsResponse result = openSearchClient.termvectors(t -> t
                    .index(testIndex.getName())
                    .id(productDocument.getId())
                    .fields("description")
            );

            // Verify the results
            assertThat(result.found()).isTrue();

            // Extract tokens and their frequencies
            Map<String, TermVector> termVectors = result.termVectors();
            String tokensAndFrequency = termVectors.entrySet().stream()
                    .flatMap(entry -> entry.getValue().terms().entrySet().stream()
                            .map(term -> term.getKey() + ":" + term.getValue().termFreq()))
                    .sorted()
                    .collect(Collectors.joining(", "));

            assertThat(tokensAndFrequency).as(explanation).isEqualTo(expectedTokensCsv);
        }
    }

    /**
     * This test creates a new custom analyzer of type "standard" (the standard analyzer), which leverages its default tokenizers and token filters
     * and applies custom stop words which will not be tokenised.
     * Read more about stop token filter here: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-stop-tokenfilter.html
     *
     * @param text              The text to index
     * @param stopWordsStr      The stop words to configure
     * @param expectedTokensCsv The expected tokens and frequencies
     * @param explanation       The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "'This is a GREAT product!', 'this,is,a', 'great:1, product:1', 'Our stopwords leave two tokens left for creation'",
            "'This is a GREAT product!', '_english_', 'great:1, product:1', 'The english stopword defaults are defined in the link above and also include those which we specified manually in the prior test case'"
    })
    public void textMapping_StandardAnalyzer_CanBeConfiguredToUseStopWordsTokenFilterToOmitWordsAsTokens(
            String text, String stopWordsStr, String expectedTokensCsv, String explanation) throws Exception {

        String customAnalyzerName = "customAnalyzer";
        String[] stopWords = stopWordsStr.split(",");

        // Define analyzer map that configures stop words
        var analyzerMap = Map.of(
                customAnalyzerName,
                new Analyzer
                        .Builder()
                        .standard(s -> s
                                .stopwords(List.of(stopWords))
                        ).build()
        );

        // Create a test index with custom analyzer and text mapping
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(
                mapping -> mapping.properties("description", Property.of(p ->
                        p.text(t -> t.analyzer(customAnalyzerName)))),
                settings -> settings.analysis(a -> a.analyzer(
                        analyzerMap)))) {

            // Create and index a product document
            ProductDocument productDocument = new ProductDocument(1, text, 1);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Get term vectors for the document
            TermvectorsResponse result = openSearchClient.termvectors(t -> t
                    .index(testIndex.getName())
                    .id(productDocument.getId())
                    .fields("description")
            );

            // Verify the results
            assertThat(result.found()).isTrue();

            // Extract tokens and their frequencies
            Map<String, TermVector> termVectors = result.termVectors();
            String tokensAndFrequency = termVectors.entrySet().stream()
                    .flatMap(entry -> entry.getValue().terms().entrySet().stream()
                            .map(term -> term.getKey() + ":" + term.getValue().termFreq()))
                    .sorted()
                    .collect(Collectors.joining(", "));

            assertThat(tokensAndFrequency).as(explanation).isEqualTo(expectedTokensCsv);
        }
    }

    /**
     * This test creates a new custom analyzer of type "standard" (the standard analyzer), which leverages its default tokenizers and token filters
     * and applies custom maximum token length.
     *
     * @param text              The text to index
     * @param maxTokenLength    The maximum token length
     * @param expectedTokensCsv The expected tokens and frequencies
     * @param explanation       The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "'GREAT product!', '3', 'at:1, duc:1, gre:1, pro:1, t:1', 'The words are decomposed into smaller tokens: great = gre + at, product = pro + duc + t'",
            "'GREAT product!', '5', 'ct:1, great:1, produ:1', 'The words are decomposed into smaller tokens: great = great, product = produ + ct'"
    })
    public void textMapping_StandardAnalyzer_CanBeConfiguredWithMaximumTokenLength(
            String text, int maxTokenLength, String expectedTokensCsv, String explanation) throws Exception {

        String customAnalyzerName = "customAnalyzer";

        // Define an analyzer map that configures max token length
        var analyzerMap = Map.of(
                customAnalyzerName,
                new Analyzer
                        .Builder()
                        .standard(s -> s
                                .maxTokenLength(maxTokenLength))
                        .build()
        );

        // Create a test index with custom analyzer and text mapping
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(
                mapping -> mapping.properties("description", Property.of(p ->
                        p.text(t -> t.analyzer(customAnalyzerName)))),
                settings -> settings.analysis(a -> a.analyzer(analyzerMap)))) {

            // Create and index a product document
            ProductDocument productDocument = new ProductDocument(1, text, 1);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Get term vectors for the document
            TermvectorsResponse result = openSearchClient.termvectors(t -> t
                    .index(testIndex.getName())
                    .id(productDocument.getId())
                    .fields("description")
            );

            // Verify the results
            assertThat(result.found()).isTrue();

            // Extract tokens and their frequencies
            Map<String, TermVector> termVectors = result.termVectors();
            String tokensAndFrequency = termVectors.entrySet().stream()
                    .flatMap(entry -> entry.getValue().terms().entrySet().stream()
                            .map(term -> term.getKey() + ":" + term.getValue().termFreq()))
                    .sorted()
                    .collect(Collectors.joining(", "));

            assertThat(tokensAndFrequency).as(explanation).isEqualTo(expectedTokensCsv);
        }
    }

    /**
     * This test performs a term query on text mapping to observe the results.
     *
     * @param text        The text to index and search for
     * @param explanation The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "'product', true, 'The standard analyzer does not produce additional tokens for single word strings'",
            "'great product', false, 'Two individual tokens are produced by the standard analyzer as it provides grammar based tokenisation. This does not provide a match, as the term query is looking to match a single token, great product, that doesn't exist'",
            "'This is a really amazing product. It is great, you absolutely must buy it!', false, 'Many tokens are produced. Grammar is stripped so that only words are indexed. Case is normalised to lowercase. Recurring words are counted against the same token. Same non-matching commentary applies as with above test'"
    })
    public void textMapping_TermQuery_DoesSomething(String text, boolean isFound, String explanation) throws Exception {
        // Create a test index with text mapping for the Description field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("description", Property.of(p -> p.text(t -> t))))) {

            // Create and index a product document
            ProductDocument productDocument = new ProductDocument(1, text, 1);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Search for documents with a term query
            SearchResponse<ProductDocument> result = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("description")
                                            .value(FieldValue.of(text))
                                    )
                            ),
                    ProductDocument.class
            );

            // Verify the results
            assertThat(result.hits().total().value() > 0).isEqualTo(isFound);
        }
    }
} 