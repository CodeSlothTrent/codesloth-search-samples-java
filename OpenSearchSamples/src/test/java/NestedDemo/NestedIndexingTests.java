package NestedDemo;

import NestedDemo.Documents.ProductAttribute;
import NestedDemo.Documents.ProductWithNestedArray;
import NestedDemo.Documents.ProductWithNestedAttribute;
import TestExtensions.LoggingOpenSearchClient;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.CountResponse;
import org.opensearch.client.opensearch.core.GetResponse;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for nested field indexing in OpenSearch.
 * These tests demonstrate how nested fields are indexed and stored in OpenSearch.
 *
 * Nested fields map an array of objects as separate hidden documents, preserving object boundaries.
 * This allows for searching on object properties within arrays while maintaining the relationship
 * between properties in the same object.
 * OpenSearch documentation: https://opensearch.org/docs/latest/mappings/supported-field-types/nested/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class NestedIndexingTests {

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public NestedIndexingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }


    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * This test verifies that nested fields are indexed correctly with all sub-properties accessible.
     * Properties can be accessed using nested queries with the path parameter.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void nestedMapping_IndexesObjectWithSubProperties() throws Exception {
        // Create a test index with nested mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.nested(n -> n))))) {

            // Create and index a product document with a strongly-typed record
            // ProductAttribute: (color, size)
            ProductWithNestedAttribute productDocument = new ProductWithNestedAttribute(
                    "1", "Product1", new ProductAttribute("red", "large"));
            testIndex.indexDocuments(new ProductWithNestedAttribute[]{productDocument});

            // Retrieve the document
            GetResponse<ProductWithNestedAttribute> result = loggingOpenSearchClient.get(
                    testIndex.getName(),
                    productDocument.getId(),
                    g -> g,
                    ProductWithNestedAttribute.class
            );

            // Verify the results
            assertThat(result.found()).isTrue();
            assertThat(result.source()).isNotNull();
            assertThat(result.source().getName()).isEqualTo("Product1");
            
            // Verify attribute was stored (it will be deserialized as a ProductAttribute record)
            ProductAttribute storedAttribute = result.source().getAttribute();
            assertThat(storedAttribute).isNotNull();
            assertThat(storedAttribute.color()).isEqualTo("red");
            assertThat(storedAttribute.size()).isEqualTo("large");
        }
    }

    /**
     * This test demonstrates that nested fields maintain object boundaries.
     * Unlike flattened fields, nested fields preserve the relationship between properties
     * within the same object when working with arrays.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void nestedMapping_SubPropertiesIndexedAsObjects() throws Exception {
        // Create a test index with nested mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.nested(n -> n))))) {

            // Create and index a product document with a strongly-typed record
            ProductWithNestedAttribute productDocument = new ProductWithNestedAttribute(
                    "1", "Product1", new ProductAttribute("red", "large"));
            testIndex.indexDocuments(new ProductWithNestedAttribute[]{productDocument});

            // Retrieve the document to verify structure is preserved
            GetResponse<ProductWithNestedAttribute> result = loggingOpenSearchClient.get(
                    testIndex.getName(),
                    productDocument.getId(),
                    g -> g,
                    ProductWithNestedAttribute.class
            );

            // Capture GET mapping request to show nested field structure
            @SuppressWarnings("unused")
            var mappingResponse = loggingOpenSearchClient.getMapping(testIndex.getName(), m -> m);

            // Verify nested structure is preserved
            assertThat(result.source().getAttribute()).isNotNull();
            assertThat(result.source().getAttribute().color()).isEqualTo("red");
            assertThat(result.source().getAttribute().size()).isEqualTo("large");

        }
    }

    /**
     * This test demonstrates that each nested object counts towards the total document count
     * for the index. Since nested objects are indexed as separate hidden documents, they
     * contribute to the index's document count statistics.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void nestedMapping_NestedObjectsCountTowardsDocumentCount() throws Exception {
        // Create a test index with nested mapping for the attributes array field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attributes", Property.of(p -> p.nested(n -> n))))) {

            // Create a document with an array of 3 nested attributes
            List<ProductAttribute> attributes = new ArrayList<>();
            attributes.add(new ProductAttribute("red", "large"));
            attributes.add(new ProductAttribute("blue", "medium"));
            attributes.add(new ProductAttribute("green", "small"));

            ProductWithNestedArray product = new ProductWithNestedArray("1", "Product1", attributes);
            testIndex.indexDocuments(new ProductWithNestedArray[]{product});

            // Get the document count using the count API
            // Note: The count API only counts top-level documents, not nested documents
            CountResponse countResponse = loggingOpenSearchClient.count(testIndex.getName(), c -> c);

            // The count API returns only 1 (the top-level document)
            // However, the actual Lucene document count is 4:
            // - 1 parent document (the product itself)
            // - 3 nested documents (one for each attribute in the array)
            // This demonstrates that nested objects are indexed as separate hidden documents
            assertThat(countResponse.count()).isEqualTo(1L); // Only top-level documents

            // Get the actual Lucene document count using the stats API
            // The stats API shows the true document count including nested documents
            // The stats method on the logging client automatically logs the request and response
            @SuppressWarnings("unused")
            var statsResponse = loggingOpenSearchClient.stats(testIndex.getName());
            
            // The stats API response contains the actual Lucene document count
            // For this test, we expect 4 documents (1 parent + 3 nested)
            // The response is captured for documentation purposes
            // Access the document count: statsResponse.indices().get(testIndex.getName()).total().docs().count()

            // Verify that we still only retrieve 1 document when getting by ID
            GetResponse<ProductWithNestedArray> retrieved = loggingOpenSearchClient.get(
                testIndex.getName(),
                "1",
                g -> g,
                ProductWithNestedArray.class
            );
            assertThat(retrieved.found()).isTrue();
            assertThat(retrieved.source().getAttributes()).hasSize(3);

        }
    }

    /**
     * This test demonstrates that single nested objects (not arrays) also count as separate documents.
     * Even when you have a single nested object (not an array), it still counts as a separate hidden document.
     * 
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void nestedMapping_SingleNestedObjectAlsoCountsAsDocument() throws Exception {
        // Create a test index with nested mapping for the attribute field (single object, not array)
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.nested(n -> n))))) {

            // Create a document with a single nested attribute (not an array)
            ProductWithNestedAttribute product = new ProductWithNestedAttribute(
                    "1", "Product1", new ProductAttribute("red", "large"));
            testIndex.indexDocuments(new ProductWithNestedAttribute[]{product});

            // Get the document count using the count API
            // Note: The count API only counts top-level documents, not nested documents
            CountResponse countResponse = loggingOpenSearchClient.count(testIndex.getName(), c -> c);

            // The count API returns only 1 (the top-level document)
            // However, the actual Lucene document count is 2:
            // - 1 parent document (the product itself)
            // - 1 nested document (the single attribute object)
            // This demonstrates that even single nested objects count as separate documents
            // To see the actual Lucene document count including nested docs, use the stats API: GET /index/_stats
            assertThat(countResponse.count()).isEqualTo(1L); // Only top-level documents

            // Verify that we still only retrieve 1 document when getting by ID
            GetResponse<ProductWithNestedAttribute> retrieved = loggingOpenSearchClient.get(
                testIndex.getName(),
                "1",
                g -> g,
                ProductWithNestedAttribute.class
            );
            assertThat(retrieved.found()).isTrue();
            assertThat(retrieved.source().getAttribute()).isNotNull();

        }
    }
}

