package KeywordDemo;

import KeywordDemo.Documents.ProductDocument;
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
import org.opensearch.client.opensearch._types.ScriptSortType;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for keyword field sorting in OpenSearch.
 * These tests demonstrate how keyword fields can be used for sorting.
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class KeywordSortingTests {
    private static final Logger logger = LogManager.getLogger(KeywordSortingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public KeywordSortingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * This test verifies that keyword fields can be used to script a sorted field.
     * <p>
     * This function is used to define a keyword mapping for the Name of a product.
     * OpenSearch documentation: https://opensearch.org/docs/2.0/opensearch/supported-field-types/keyword/
     * ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedToScriptASortedField() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "mouse", 1),
                    new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with a script sort
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .sort(s -> s
                            .script(ss -> ss
                                    .type(ScriptSortType.Number)  // Must specify the script return type
                                    .script(sc -> sc
                                            .inline(i -> i
                                                    .source("doc['name'].value == 'mouse pad' ? 0 : 1")
                                            )
                                    )
                                    .order(SortOrder.Asc)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> searchResponse = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(searchResponse.hits().total().value()).isEqualTo(2);

            // The first document should be "mouse pad" because it has a sort value of 0
            List<Hit<ProductDocument>> hits = searchResponse.hits().hits();
            assertThat(hits.get(0).source().getName()).isEqualTo("mouse pad");
            assertThat(hits.get(1).source().getName()).isEqualTo("mouse");
        }
    }

    /**
     * This test verifies that keyword fields can be used as a sorted field without any special considerations.
     * <p>
     * Keyword fields do not require anything special to support sorting.
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedAsASortedField_WithoutAnySpecialConsiderations() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "mouse", 1),
                    new ProductDocument(2, "mouse pad", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with a field sort
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .sort(s -> s
                            .field(f -> f
                                    .field("name")
                                    .order(SortOrder.Desc)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> searchResponse = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(searchResponse.hits().total().value()).isEqualTo(2);

            // Check that the documents are sorted by name (descending)
            List<String> sortedNames = searchResponse.hits().hits().stream()
                    .map(hit -> hit.source().getName())
                    .collect(Collectors.toList());

            assertThat(sortedNames).containsExactly("mouse pad", "mouse");
        }
    }

    /**
     * This test verifies that keyword fields should not be used to sort numeric data.
     *
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_ShouldNotBeUsedToSortNumericData() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {

            // Create and index product documents with numeric names
            ProductDocument[] productDocuments = new ProductDocument[]{
                    new ProductDocument(1, "5", 1),
                    new ProductDocument(2, "2000", 2)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with a field sort
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(testIndex.getName())
                    .sort(s -> s
                            .field(f -> f
                                    .field("name")
                                    .order(SortOrder.Desc)
                            )
                    )
                    .build();

            // Execute the search request
            SearchResponse<ProductDocument> searchResponse = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(searchResponse.hits().total().value()).isEqualTo(2);

            // Check that the documents are sorted by name (descending)
            List<String> sortedNames = searchResponse.hits().hits().stream()
                    .map(hit -> hit.source().getName())
                    .collect(Collectors.toList());

            // Note: Keyword fields sort lexicographically, not numerically
            // So "5" comes after "2000" in descending order
            assertThat(sortedNames).containsExactly("5", "2000");
        }
    }
} 