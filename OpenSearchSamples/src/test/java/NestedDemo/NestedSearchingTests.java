package NestedDemo;

import NestedDemo.Documents.*;
import NestedDemo.Documents.ProductAttribute;
import NestedDemo.Documents.ProductSpecification;
import NestedDemo.Documents.ProductWithNestedAttribute;
import TestExtensions.LoggingOpenSearchClient;
import TestExtensions.OpenSearchResourceManagementExtension;
import TestExtensions.OpenSearchSharedResource;
import TestInfrastructure.OpenSearchIndexFixture;
import TestInfrastructure.OpenSearchTestIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for nested field searching in OpenSearch.
 * These tests demonstrate how nested fields can be queried and their key advantages.
 *
 * Nested fields allow searching on object properties within arrays while preserving
 * object boundaries. This is the KEY ADVANTAGE over flattened fields - you can match
 * multiple properties from the same object in an array.
 *
 * OpenSearch documentation: https://opensearch.org/docs/latest/mappings/supported-field-types/nested/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class NestedSearchingTests {

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public NestedSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }


    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * Demonstrates that a single nested field can be searched using nested queries.
     * This shows how to search on individual properties within a nested object using
     * the nested query wrapper with path and query parameters.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void nestedMapping_SingleField_CanSearchByDottedNotation() throws Exception {
        // Create a test index with nested mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.nested(n -> n
                        .properties("color", Property.of(prop -> prop.keyword(k -> k)))
                        .properties("size", Property.of(prop -> prop.keyword(k -> k)))
                ))))) {

            // Create documents with attributes containing multiple properties
            ProductWithNestedAttribute[] products = new ProductWithNestedAttribute[]{
                    new ProductWithNestedAttribute("1", "Product1", new ProductAttribute("red", "large")),
                    new ProductWithNestedAttribute("2", "Product2", new ProductAttribute("blue", "medium")),
                    new ProductWithNestedAttribute("3", "Product3", new ProductAttribute("red", "small"))
            };
            testIndex.indexDocuments(products);

            // Search by attribute.color using nested query
            SearchResponse<ProductWithNestedAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .nested(n -> n
                                            .path("attribute")
                                            .query(q2 -> q2
                                                    .term(t -> t
                                                            .field("attribute.color")
                                                            .value(FieldValue.of("red"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithNestedAttribute.class
            );

            // With nested type, queries work correctly
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "3"); // Product1 and Product3 have red color

            // Search by attribute.size using nested query
            SearchResponse<ProductWithNestedAttribute> sizeResult = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .nested(n -> n
                                            .path("attribute")
                                            .query(q2 -> q2
                                                    .term(t -> t
                                                            .field("attribute.size")
                                                            .value(FieldValue.of("large"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithNestedAttribute.class
            );

            assertThat(sizeResult.hits().total().value()).isEqualTo(1);
            assertThat(sizeResult.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1"); // Only Product1 has large size

        }
    }

    /**
     * Demonstrates that you CAN match multiple properties from within the same nested field.
     * This shows that for a single nested object (not an array), you can search for multiple
     * properties using boolean queries within the nested query wrapper.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void nestedMapping_SingleField_CanMatchMultiplePropertiesFromSameObject() throws Exception {
        // Create a test index with nested mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.nested(n -> n
                        .properties("color", Property.of(prop -> prop.keyword(k -> k)))
                        .properties("size", Property.of(prop -> prop.keyword(k -> k)))
                ))))) {

            // Create documents with attributes containing multiple properties
            ProductWithNestedAttribute[] products = new ProductWithNestedAttribute[]{
                    new ProductWithNestedAttribute("1", "Product1", new ProductAttribute("red", "large")),
                    new ProductWithNestedAttribute("2", "Product2", new ProductAttribute("blue", "medium")),
                    new ProductWithNestedAttribute("3", "Product3", new ProductAttribute("red", "small")),
                    new ProductWithNestedAttribute("4", "Product4", new ProductAttribute("red", "large"))
            };
            testIndex.indexDocuments(products);

            // Search for products where attribute.color="red" AND attribute.size="large"
            // Using nested query with bool query inside
            SearchResponse<ProductWithNestedAttribute> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .nested(n -> n
                                            .path("attribute")
                                            .query(q2 -> q2
                                                    .bool(b -> b
                                                            .must(m -> m
                                                                    .term(t -> t
                                                                            .field("attribute.color")
                                                                            .value(FieldValue.of("red"))
                                                                    )
                                                            )
                                                            .must(m -> m
                                                                    .term(t -> t
                                                                            .field("attribute.size")
                                                                            .value(FieldValue.of("large"))
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            ),
                    ProductWithNestedAttribute.class
            );

            // With nested type, matching multiple properties from the same object works correctly
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "4"); // Product1 and Product4 match: red AND large

        }
    }

    /**
     * Demonstrates that you CAN combine multiple top-level nested queries for different paths.
     * 
     * This test demonstrates:
     * 1. Individual nested queries on each field work correctly
     * 2. Combining them in a bool.must query works correctly and properly intersects results
     * 
     * You can search on multiple nested fields independently, and combining multiple
     * top-level nested queries for DIFFERENT paths in a bool query works as expected.
     * OpenSearch correctly intersects the results from multiple independent nested queries
     * targeting different paths at the parent document level.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void nestedMapping_MultipleNestedFields_CanMatchFromBothFields() throws Exception {
        // Create a test index with multiple nested fields
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping -> {
                    mapping.properties("primaryAttribute", Property.of(p -> p.nested(n -> n
                            .properties("color", Property.of(prop -> prop.keyword(k -> k)))
                            .properties("size", Property.of(prop -> prop.keyword(k -> k)))
                    )));
                    mapping.properties("secondaryAttribute", Property.of(p -> p.nested(n -> n
                            .properties("brand", Property.of(prop -> prop.keyword(k -> k)))
                            .properties("category", Property.of(prop -> prop.keyword(k -> k)))
                    )));
                })) {

            // Create documents with both primary and secondary attributes using different DTO types
            ProductWithTwoNestedAttributes[] products = new ProductWithTwoNestedAttributes[]{
                    new ProductWithTwoNestedAttributes("1", "Product1", 
                            new ProductAttribute("red", "large"),
                            new ProductSpecification("BrandA", "Electronics")),
                    new ProductWithTwoNestedAttributes("2", "Product2",
                            new ProductAttribute("red", "medium"),
                            new ProductSpecification("BrandB", "Clothing")),
                    new ProductWithTwoNestedAttributes("3", "Product3",
                            new ProductAttribute("blue", "small"),
                            new ProductSpecification("BrandA", "Electronics"))
            };
            testIndex.indexDocuments(products);

            // First verify individual nested queries work correctly
            SearchResponse<ProductWithTwoNestedAttributes> primaryOnlyResult = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .nested(n -> n
                                            .path("primaryAttribute")
                                            .query(q2 -> q2
                                                    .term(t -> t
                                                            .field("primaryAttribute.color")
                                                            .value(FieldValue.of("red"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithTwoNestedAttributes.class
            );

            // Should match Product1 and Product2 (both have red color)
            assertThat(primaryOnlyResult.hits().total().value()).isEqualTo(2);

            // Verify secondaryAttribute query also works independently
            SearchResponse<ProductWithTwoNestedAttributes> secondaryOnlyResult = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .nested(n -> n
                                            .path("secondaryAttribute")
                                            .query(q2 -> q2
                                                    .term(t -> t
                                                            .field("secondaryAttribute.brand")
                                                            .value(FieldValue.of("BrandA"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithTwoNestedAttributes.class
            );

            // Should match Product1 and Product3 (both have BrandA)
            assertThat(secondaryOnlyResult.hits().total().value()).isEqualTo(2);

            // Search for products with primaryAttribute.color="red" AND secondaryAttribute.brand="BrandA"
            // Using multiple nested queries combined with bool - THIS WORKS CORRECTLY
            // 
            // Expected: Should match Product1 (has red color AND BrandA)
            // Actual: Returns 1 result (Product1) - correctly intersects both nested queries
            //
            // OpenSearch properly intersects results from multiple independent nested queries
            // targeting different paths at the parent document level.
            SearchResponse<ProductWithTwoNestedAttributes> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .bool(b -> b
                                            .must(m -> m
                                                    .nested(n -> n
                                                            .path("primaryAttribute")
                                                            .query(q2 -> q2
                                                                    .term(t -> t
                                                                            .field("primaryAttribute.color")
                                                                            .value(FieldValue.of("red"))
                                                                    )
                                                            )
                                                    )
                                            )
                                            .must(m -> m
                                                    .nested(n -> n
                                                            .path("secondaryAttribute")
                                                            .query(q2 -> q2
                                                                    .term(t -> t
                                                                            .field("secondaryAttribute.brand")
                                                                            .value(FieldValue.of("BrandA"))
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            ),
                    ProductWithTwoNestedAttributes.class
            );

            // This compound query correctly returns 1 result (Product1):
            // - Product1 has primaryAttribute.color="red" AND secondaryAttribute.brand="BrandA" - MATCHES
            // - Product2 has red color but BrandB, so it correctly won't match
            // - Product3 has BrandA but blue color, so it correctly won't match
            //
            // OpenSearch properly intersects results from multiple independent nested queries
            // targeting different paths at the parent document level.
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1"); // Only Product1 matches both conditions

        }
    }

    /**
     * Demonstrates searching on nested nested fields using multiple levels of nested queries.
     * This shows how to access leaf properties when a nested field contains nested complex types.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void nestedMapping_NestedNestedField_CanSearchByNestedDottedNotation() throws Exception {
        // Create a test index with nested mapping for the details field
        // The attribute field is also nested, not object, requiring nested -> query -> nested -> query structure
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("details", Property.of(p -> p.nested(n -> n
                        .properties("attribute", Property.of(attr -> attr.nested(n2 -> n2
                                .properties("color", Property.of(prop -> prop.keyword(k -> k)))
                                .properties("size", Property.of(prop -> prop.keyword(k -> k)))
                        )))
                        .properties("description", Property.of(prop -> prop.keyword(k -> k)))
                ))))) {

            // Create documents with nested attribute structures
            ProductWithNestedDetails[] products = new ProductWithNestedDetails[]{
                    new ProductWithNestedDetails("1", "Product1",
                            new ProductDetails(
                                    new ProductAttribute("red", "large"),
                                    "Premium quality product")),
                    new ProductWithNestedDetails("2", "Product2",
                            new ProductDetails(
                                    new ProductAttribute("blue", "medium"),
                                    "Standard quality product")),
                    new ProductWithNestedDetails("3", "Product3",
                            new ProductDetails(
                                    new ProductAttribute("red", "small"),
                                    "Compact design"))
            };
            testIndex.indexDocuments(products);

            // Search using nested -> query -> nested -> query structure: details.attribute.color
            SearchResponse<ProductWithNestedDetails> result = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .nested(n -> n
                                            .path("details")
                                            .query(q2 -> q2
                                                    .nested(n2 -> n2
                                                            .path("details.attribute")
                                                            .query(q3 -> q3
                                                                    .term(t -> t
                                                                            .field("details.attribute.color")
                                                                            .value(FieldValue.of("red"))
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            ),
                    ProductWithNestedDetails.class
            );

            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "3");

        }
    }

    /**
     * **CRITICAL TEST**: Demonstrates nested fields' KEY ADVANTAGE - matching multiple values
     * from the SAME object in an array.
     * 
     * When you have an array of nested objects, nested fields preserve object boundaries,
     * allowing you to match multiple properties from the same array object.
     * 
     * This is where nested fields SHINE - flattened arrays cannot do this reliably because
     * they lose object boundaries.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void nestedMapping_ArrayOfObjects_CanMatchMultipleValuesFromSameObject() throws Exception {
        // Create a test index with nested mapping for the attributes array field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attributes", Property.of(p -> p.nested(n -> n
                        .properties("color", Property.of(prop -> prop.keyword(k -> k)))
                        .properties("size", Property.of(prop -> prop.keyword(k -> k)))
                ))))) {

            // Create a document with an array of attributes where each object has multiple properties
            List<ProductAttribute> attributes = new ArrayList<>();
            attributes.add(new ProductAttribute("red", "large"));  // Object 1: red and large
            attributes.add(new ProductAttribute("blue", "small"));   // Object 2: blue and small

            ProductWithNestedArray[] products = new ProductWithNestedArray[]{
                    new ProductWithNestedArray("1", "Product1", attributes)
            };
            testIndex.indexDocuments(products);

            // **KEY TEST**: Search for color="red" AND size="large" from the SAME object
            // This WILL work with nested fields because they preserve object boundaries
            SearchResponse<ProductWithNestedArray> sameObjectResult = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .nested(n -> n
                                            .path("attributes")
                                            .query(q2 -> q2
                                                    .bool(b -> b
                                                            .must(m -> m
                                                                    .term(t -> t
                                                                            .field("attributes.color")
                                                                            .value(FieldValue.of("red"))
                                                                    )
                                                            )
                                                            .must(m -> m
                                                                    .term(t -> t
                                                                            .field("attributes.size")
                                                                            .value(FieldValue.of("large"))
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            ),
                    ProductWithNestedArray.class
            );

            // This SHOULD match because both "red" and "large" exist in the SAME array object
            // This is the key advantage of nested fields over flattened fields
            assertThat(sameObjectResult.hits().total().value()).isEqualTo(1);
            assertThat(sameObjectResult.hits().hits().get(0).source().getId()).isEqualTo("1");

            // Now test the OPPOSITE: Search for color="red" AND size="small" from DIFFERENT objects
            // This should NOT match because nested fields preserve object boundaries
            SearchResponse<ProductWithNestedArray> differentObjectsResult = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .nested(n -> n
                                            .path("attributes")
                                            .query(q2 -> q2
                                                    .bool(b -> b
                                                            .must(m -> m
                                                                    .term(t -> t
                                                                            .field("attributes.color")
                                                                            .value(FieldValue.of("red"))
                                                                    )
                                                            )
                                                            .must(m -> m
                                                                    .term(t -> t
                                                                            .field("attributes.size")
                                                                            .value(FieldValue.of("small"))
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            ),
                    ProductWithNestedArray.class
            );

            // This should NOT match because "red" and "small" are in different objects
            assertThat(differentObjectsResult.hits().total().value()).isEqualTo(0);
            assertThat(differentObjectsResult.hits().hits()).isEmpty();

            // Verify that individual searches also work
            SearchResponse<ProductWithNestedArray> colorResult = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .nested(n -> n
                                            .path("attributes")
                                            .query(q2 -> q2
                                                    .term(t -> t
                                                            .field("attributes.color")
                                                            .value(FieldValue.of("red"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithNestedArray.class
            );

            assertThat(colorResult.hits().total().value()).isEqualTo(1);
            
            // This demonstrates that nested arrays preserve object boundaries:
            // - Matching values from the SAME object works (as shown above)
            // - Matching values from DIFFERENT objects correctly fails
            // This is the KEY ADVANTAGE over flattened arrays

        }
    }
}

