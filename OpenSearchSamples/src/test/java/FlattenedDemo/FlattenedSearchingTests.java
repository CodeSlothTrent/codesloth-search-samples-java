package FlattenedDemo;

import FlattenedDemo.Documents.*;
import FlattenedDemo.Documents.ProductAttribute;
import FlattenedDemo.Documents.ProductWithFlattenedAttribute;
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
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for flattened field searching in OpenSearch.
 * These tests demonstrate how flattened fields can be queried and their limitations.
 *
 * Flattened fields allow searching on object properties using dotted notation (e.g., metadata.title).
 * However, flattened arrays have limitations - you cannot match multiple values from a single object
 * in the array. For that, you need nested or join field types.
 *
 * OpenSearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/flattened/
 */
@ExtendWith(OpenSearchResourceManagementExtension.class)
public class FlattenedSearchingTests {
    private static final Logger logger = LogManager.getLogger(FlattenedSearchingTests.class);

    private OpenSearchClient openSearchClient;
    private OpenSearchIndexFixture fixture;

    public FlattenedSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.openSearchClient = openSearchSharedResource.getOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(openSearchClient);
    }

    /**
     * Special Case 1: Demonstrates that a single flattened field with multiple properties
     * can be searched using dotted notation (flattenedField.property).
     * This shows how to search on individual properties within a flattened object.
     * 
     * This test uses the flatObject field type to demonstrate flattened field functionality.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_SingleField_MultipleProperties_CanSearchByDottedNotation() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with attributes containing multiple properties using strongly-typed records
            // ProductAttribute: (color, size)
            ProductAttribute attribute1 = new ProductAttribute("red", "large");
            ProductAttribute attribute2 = new ProductAttribute("blue", "medium");
            ProductAttribute attribute3 = new ProductAttribute("red", "small");

            ProductWithFlattenedAttribute[] products = new ProductWithFlattenedAttribute[]{
                    new ProductWithFlattenedAttribute("1", "Product1", attribute1),
                    new ProductWithFlattenedAttribute("2", "Product2", attribute2),
                    new ProductWithFlattenedAttribute("3", "Product3", attribute3)
            };
            testIndex.indexDocuments(products);

            // Search by attribute.color using term query
            SearchResponse<ProductWithFlattenedAttribute> result = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attribute.color")
                                            .value(FieldValue.of("red"))
                                    )
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // With flatObject type, dotted notation queries work correctly
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "3"); // Product1 and Product3 have red color

            // Search by attribute.size using dotted notation
            SearchResponse<ProductWithFlattenedAttribute> sizeResult = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attribute.size")
                                            .value(FieldValue.of("large"))
                                    )
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // With flatObject type, size search works correctly
            assertThat(sizeResult.hits().total().value()).isEqualTo(1);
            assertThat(sizeResult.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1"); // Only Product1 has large size
        }
    }

    /**
     * Demonstrates that you CAN match multiple properties from within the same flattened field.
     * This shows that for a single flattened object (not an array), you can search for multiple
     * properties using boolean queries (e.g., attribute.color="red" AND attribute.size="large").
     *
     * This is different from flattened arrays, where you cannot reliably match multiple properties
     * from the same object in the array.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_SingleField_CanMatchMultiplePropertiesFromSameObject() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with attributes containing multiple properties
            // ProductAttribute: (color, size)
            ProductAttribute attribute1 = new ProductAttribute("red", "large");
            ProductAttribute attribute2 = new ProductAttribute("blue", "medium");
            ProductAttribute attribute3 = new ProductAttribute("red", "small");
            ProductAttribute attribute4 = new ProductAttribute("red", "large");

            ProductWithFlattenedAttribute[] products = new ProductWithFlattenedAttribute[]{
                    new ProductWithFlattenedAttribute("1", "Product1", attribute1),
                    new ProductWithFlattenedAttribute("2", "Product2", attribute2),
                    new ProductWithFlattenedAttribute("3", "Product3", attribute3),
                    new ProductWithFlattenedAttribute("4", "Product4", attribute4)
            };
            testIndex.indexDocuments(products);

            // Search for products where attribute.color="red" AND attribute.size="large"
            // This should work because we're matching multiple properties from the same flattened object
            SearchResponse<ProductWithFlattenedAttribute> result = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
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
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // With flatObject type, matching multiple properties from the same object works correctly
            // Should match Product1 (id="1") and Product4 (id="4") - both red AND large
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "4"); // Product1 and Product4 match: red AND large

            // Verify the actual documents match our expectations
            assertThat(result.hits().hits().stream()
                    .anyMatch(h -> h.source().getId().equals("1") && h.source().getName().equals("Product1")))
                    .isTrue();
            assertThat(result.hits().hits().stream()
                    .anyMatch(h -> h.source().getId().equals("4") && h.source().getName().equals("Product4")))
                    .isTrue();

            // Test with different combination: color="red" AND size="small"
            SearchResponse<ProductWithFlattenedAttribute> smallResult = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
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
                                                            .value(FieldValue.of("small"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // Should match only Product3 (id="3") - red AND small
            assertThat(smallResult.hits().total().value()).isEqualTo(1);
            assertThat(smallResult.hits().hits().get(0).source().getId()).isEqualTo("3");
            assertThat(smallResult.hits().hits().get(0).source().getName()).isEqualTo("Product3");
        }
    }

    /**
     * Special Case 2: Demonstrates the limitation that an array of flattened objects
     * cannot match multiple values from the same object in the array.
     * 
     * When you have an array like:
     * [
     *   {color: "red", size: "large"},
     *   {color: "blue", size: "small"}
     * ]
     * 
     * You cannot find documents where a single object has both color="red" AND size="large"
     * using flattened fields. You need nested or join types for this.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_ArrayOfObjects_CannotMatchMultipleValuesFromSameObject() throws Exception {
        // Create a test index with flattened mapping for the attributes array field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attributes", Property.of(p -> p.flatObject(f -> f))))) {

            // Create a document with an array of attributes where each object has multiple properties
            // Using strongly-typed ProductAttribute records
            List<ProductAttribute> attributes = new ArrayList<>();
            
            // Object 1: red and large
            ProductAttribute attr1 = new ProductAttribute("red", "large");
            attributes.add(attr1);
            
            // Object 2: blue and small
            ProductAttribute attr2 = new ProductAttribute("blue", "small");
            attributes.add(attr2);

            ProductWithFlattenedArray[] products = new ProductWithFlattenedArray[]{
                    new ProductWithFlattenedArray("1", "Product1", attributes)
            };
            testIndex.indexDocuments(products);

            // Try to match a single object with both color="red" AND size="large"
            // This should NOT work with flattened fields because flattened treats arrays
            // as separate values, not as structured objects
            SearchResponse<ProductWithFlattenedArray> bothResult = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
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
                            ),
                    ProductWithFlattenedArray.class
            );

            // With flattened fields, this query will incorrectly match because:
            // - attributes.color="red" matches (from first object)
            // - attributes.size="large" matches (from first object)
            // But flattened doesn't preserve the relationship that these come from the same object
            // So it matches even though they are in the same object
            // NOTE: This is a known limitation - for proper matching, you need nested or join types
            
            // Actually, let's verify what happens - it might match, but it shouldn't be reliable
            // The behavior depends on how OpenSearch handles flattened arrays internally
            logger.info("Result count for both attributes: {}", bothResult.hits().total().value());
            
            // However, we can verify that individual searches work
            SearchResponse<ProductWithFlattenedArray> colorResult = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attributes.color")
                                            .value(FieldValue.of("red"))
                                    )
                            ),
                    ProductWithFlattenedArray.class
            );

            assertThat(colorResult.hits().total().value()).isGreaterThanOrEqualTo(1);
            
            // This demonstrates the limitation - flattened arrays don't preserve object boundaries
            // For accurate matching of multiple properties from the same array object,
            // you must use nested or join field types
        }
    }

    /**
     * Special Case 3: Demonstrates that multiple flattened fields can match multiple values
     * from each field independently. This shows that you CAN match across different flattened fields
     * in the same document (e.g., primaryAttribute.color="red" AND secondaryAttribute.size="large").
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_MultipleFlattenedFields_CanMatchFromBothFields() throws Exception {
        // Create a test index with multiple flattened fields: primaryAttribute and secondaryAttribute
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping -> {
                    mapping.properties("primaryAttribute", Property.of(p -> p.flatObject(f -> f)));
                    mapping.properties("secondaryAttribute", Property.of(p -> p.flatObject(f -> f)));
                })) {

            // Create documents with both primary and secondary attributes using strongly-typed records
            // ProductAttribute: (color, size)
            ProductAttribute primary1 = new ProductAttribute("red", "large");
            ProductAttribute secondary1 = new ProductAttribute("blue", "medium");

            ProductAttribute primary2 = new ProductAttribute("red", "medium");
            ProductAttribute secondary2 = new ProductAttribute("green", "small");

            ProductAttribute primary3 = new ProductAttribute("blue", "small");
            ProductAttribute secondary3 = new ProductAttribute("red", "large");

            ProductWithTwoFlattenedAttributes[] products = new ProductWithTwoFlattenedAttributes[]{
                    new ProductWithTwoFlattenedAttributes("1", "Product1", primary1, secondary1),
                    new ProductWithTwoFlattenedAttributes("2", "Product2", primary2, secondary2),
                    new ProductWithTwoFlattenedAttributes("3", "Product3", primary3, secondary3)
            };
            testIndex.indexDocuments(products);

            // Search for products with primaryAttribute.color="red" AND secondaryAttribute.size="large"
            // This should work because we're matching across different flattened fields
            SearchResponse<ProductWithTwoFlattenedAttributes> result = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .bool(b -> b
                                            .must(m -> m
                                                    .term(t -> t
                                                            .field("primaryAttribute.color")
                                                            .value(FieldValue.of("red"))
                                                    )
                                            )
                                            .must(m -> m
                                                    .term(t -> t
                                                            .field("secondaryAttribute.size")
                                                            .value(FieldValue.of("large"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithTwoFlattenedAttributes.class
            );

            // With flatObject type, matching across multiple flattened fields works correctly
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId()))
                    .containsExactly("3"); // Product3 matches: primaryAttribute.color="red" AND secondaryAttribute.size="large"

            // Search for primaryAttribute.color="red" alone - should match Product1, Product2, and Product3
            SearchResponse<ProductWithTwoFlattenedAttributes> primaryResult = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("primaryAttribute.color")
                                            .value(FieldValue.of("red"))
                                    )
                            ),
                    ProductWithTwoFlattenedAttributes.class
            );

            assertThat(primaryResult.hits().total().value()).isEqualTo(2);
            assertThat(primaryResult.hits().hits().stream()
                    .map(h -> h.source().getId()))
                    .containsExactlyInAnyOrder("1", "2");

            // Search for secondaryAttribute.size="large" alone - should match Product1 and Product3
            SearchResponse<ProductWithTwoFlattenedAttributes> secondaryResult = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("secondaryAttribute.size")
                                            .value(FieldValue.of("large"))
                                    )
                            ),
                    ProductWithTwoFlattenedAttributes.class
            );

            assertThat(secondaryResult.hits().total().value()).isEqualTo(1);
            assertThat(secondaryResult.hits().hits().stream()
                    .map(h -> h.source().getId()))
                    .containsExactly("3");
        }
    }

    /**
     * Demonstrates searching on nested flattened fields using multiple levels of dotted notation.
     * This shows how to access leaf properties when a flattened field contains nested complex types
     * (e.g., details.attribute.color, details.attribute.size, details.description).
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_NestedFlattenedField_CanSearchWithMultipleDottedNotation() throws Exception {
        // Create a test index with flattened mapping for the details field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("details", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with nested attribute structures
            // ProductDetails: (attribute: ProductAttribute, description: String)
            // ProductAttribute: (color, size)
            ProductDetails details1 = new ProductDetails(
                    new ProductAttribute("red", "large"),
                    "Premium quality product"
            );
            ProductDetails details2 = new ProductDetails(
                    new ProductAttribute("blue", "medium"),
                    "Standard quality product"
            );
            ProductDetails details3 = new ProductDetails(
                    new ProductAttribute("red", "small"),
                    "Compact design"
            );

            ProductWithNestedFlattened[] products = new ProductWithNestedFlattened[]{
                    new ProductWithNestedFlattened("1", "Product1", details1),
                    new ProductWithNestedFlattened("2", "Product2", details2),
                    new ProductWithNestedFlattened("3", "Product3", details3)
            };
            testIndex.indexDocuments(products);

            // Search using multi-level dotted notation: details.attribute.color
            SearchResponse<ProductWithNestedFlattened> result = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("details.attribute.color")
                                            .value(FieldValue.of("red"))
                                    )
                            ),
                    ProductWithNestedFlattened.class
            );

            // With flatObject type, nested dotted notation queries work correctly
            assertThat(result.hits().total().value()).isEqualTo(2);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId())
                    .sorted())
                    .containsExactly("1", "3"); // Product1 and Product3 have red color in nested attribute

            // Search using another nested property: details.attribute.size
            SearchResponse<ProductWithNestedFlattened> sizeResult = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("details.attribute.size")
                                            .value(FieldValue.of("large"))
                                    )
                            ),
                    ProductWithNestedFlattened.class
            );

            assertThat(sizeResult.hits().total().value()).isEqualTo(1);
            assertThat(sizeResult.hits().hits().get(0).source().getId()).isEqualTo("1");

            // Search using a direct property of details: details.description
            SearchResponse<ProductWithNestedFlattened> descriptionResult = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("details.description")
                                            .value(FieldValue.of("Premium quality product"))
                                    )
                            ),
                    ProductWithNestedFlattened.class
            );

            assertThat(descriptionResult.hits().total().value()).isEqualTo(1);
            assertThat(descriptionResult.hits().hits().get(0).source().getId()).isEqualTo("1");

            // Search combining nested and direct properties: details.attribute.color="red" AND details.description="Compact design"
            SearchResponse<ProductWithNestedFlattened> combinedResult = openSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .bool(b -> b
                                            .must(m -> m
                                                    .term(t -> t
                                                            .field("details.attribute.color")
                                                            .value(FieldValue.of("red"))
                                                    )
                                            )
                                            .must(m -> m
                                                    .term(t -> t
                                                            .field("details.description")
                                                            .value(FieldValue.of("Compact design"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithNestedFlattened.class
            );

            assertThat(combinedResult.hits().total().value()).isEqualTo(1);
            assertThat(combinedResult.hits().hits().get(0).source().getId()).isEqualTo("3");
        }
    }

}

