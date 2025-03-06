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
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
     * 
     * @throws IOException If an I/O error occurs
     */
    @Test
    public void keywordMapping_CanBeUsedToScriptASortedField() throws Exception {
        // Create a test index with keyword mapping for the Name field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping -> 
            mapping.properties("name", Property.of(p -> p.keyword(k -> k))))) {
            
            // Create and index product documents
            ProductDocument[] productDocuments = new ProductDocument[] {
                new ProductDocument(1, "mouse", 1),
                new ProductDocument(2, "keyboard", 2),
                new ProductDocument(3, "monitor", 3)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with a script sort
            SearchRequest searchRequest = new SearchRequest.Builder()
                .index(testIndex.getName())
                .sort(s -> s
                    .script(ss -> ss
                        .script(sc -> sc
                            .inline(i -> i
                                .source("doc['name'].value.length()")
                            )
                        )
                        .order(SortOrder.Asc)
                    )
                )
                .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.hits().hits()).hasSize(3);
            
            // Check that the documents are sorted by name length (ascending)
            List<String> sortedNames = response.hits().hits().stream()
                .map(hit -> hit.source().getName())
                .collect(Collectors.toList());
            
            assertThat(sortedNames).containsExactly("mouse", "monitor", "keyboard");
        }
    }

    /**
     * This test verifies that keyword fields can be used as a sorted field without any special considerations.
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
            ProductDocument[] productDocuments = new ProductDocument[] {
                new ProductDocument(1, "mouse", 1),
                new ProductDocument(2, "keyboard", 2),
                new ProductDocument(3, "monitor", 3)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with a field sort
            SearchRequest searchRequest = new SearchRequest.Builder()
                .index(testIndex.getName())
                .sort(s -> s
                    .field(f -> f
                        .field("name")
                        .order(SortOrder.Asc)
                    )
                )
                .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.hits().hits()).hasSize(3);
            
            // Check that the documents are sorted by name (ascending)
            List<String> sortedNames = response.hits().hits().stream()
                .map(hit -> hit.source().getName())
                .collect(Collectors.toList());
            
            assertThat(sortedNames).containsExactly("keyboard", "monitor", "mouse");
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
            ProductDocument[] productDocuments = new ProductDocument[] {
                new ProductDocument(1, "1", 1),
                new ProductDocument(2, "2", 2),
                new ProductDocument(3, "10", 3)
            };
            testIndex.indexDocuments(productDocuments);

            // Create a search request with a field sort
            SearchRequest searchRequest = new SearchRequest.Builder()
                .index(testIndex.getName())
                .sort(s -> s
                    .field(f -> f
                        .field("name")
                        .order(SortOrder.Asc)
                    )
                )
                .build();

            // Execute the search request
            SearchResponse<ProductDocument> response = openSearchClient.search(searchRequest, ProductDocument.class);

            // Verify the results
            assertThat(response.hits().hits()).hasSize(3);
            
            // Check that the documents are sorted by name lexicographically, not numerically
            // This means "1", "10", "2" instead of "1", "2", "10"
            List<String> sortedNames = response.hits().hits().stream()
                .map(hit -> hit.source().getName())
                .collect(Collectors.toList());
            
            assertThat(sortedNames).containsExactly("1", "10", "2");
        }
    }
} 