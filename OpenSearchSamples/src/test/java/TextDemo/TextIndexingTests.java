package TextDemo;

import TestExtensions.LoggingOpenSearchClient;
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
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.TermvectorsResponse;
import org.opensearch.client.opensearch.core.termvectors.TermVector;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for text field indexing in OpenSearch.
 * These tests demonstrate how text fields are indexed and stored in OpenSearch.
 * 
 * @see KeywordDemo.KeywordIndexingTests to compare how keywords differ to text fields
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class TextIndexingTests {
    private static final Logger logger = LogManager.getLogger(TextIndexingTests.class);

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public TextIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that text fields are tokenized using the standard analyzer.
     * 
     * This function is used to define a text mapping for the description of a product.
     * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/text/
     * ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
     *
     * @param description       The text to index
     * @param expectedTokensStr The expected tokens and frequencies
     * @param explanation       The explanation of the test case
     * @throws IOException If an I/O error occurs
     */
    @ParameterizedTest
    @CsvSource({
            "'Mouse', 'mouse:1', 'Single word is indexed exactly as given'",
            "'Mouse pad', 'mouse:1,pad:1', 'Two words are stored as separate tokens'",
            "'This is a sentence! It contains some, really bad. Grammar; sentence', 'a:1,bad:1,contains:1,grammar:1,is:1,it:1,really:1,sentence:2,some:1,this:1', 'Grammar is removed and whole words are stored as tokens, lowercase normalised'"
    })
    public void textMapping_IndexesUsingStandardTokensiserForGivenString(String description, String expectedTokensStr, String explanation) throws Exception {
        // Create a test index with text mapping for the Description field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("description", Property.of(p -> p.text(t -> t))))) {

            // Create and index a product document
            ProductDocument productDocument = new ProductDocument(1, description, 1);
            testIndex.indexDocuments(new ProductDocument[]{productDocument});

            // Get term vectors for the document
            TermvectorsResponse result = loggingOpenSearchClient.getClient().termvectors(t -> t
                    .index(testIndex.getName())
                    .id(String.valueOf(productDocument.getId()))
                    .fields("description")
            );

            // Verify the results
            assertThat(result.found()).isTrue();

            // Extract tokens and their frequencies
            Map<String, TermVector> termVectors = result.termVectors();
            List<String> tokensAndFrequency = termVectors.entrySet().stream()
                    .flatMap(entry -> entry.getValue().terms().entrySet().stream()
                            .map(term -> term.getKey() + ":" + term.getValue().termFreq()))
                    .collect(Collectors.toList());

            List<String> expectedTokensList = Arrays.asList(expectedTokensStr.split(","));
            assertThat(tokensAndFrequency).as(explanation).containsExactlyInAnyOrderElementsOf(expectedTokensList);
        }
    }
} 