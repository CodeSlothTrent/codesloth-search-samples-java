package FlattenedDemo;

import FlattenedDemo.Documents.*;
import FlattenedDemo.Documents.ProductAttribute;
import FlattenedDemo.Documents.ProductSpecification;
import FlattenedDemo.Documents.ProductWithFlattenedAttribute;
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

    private LoggingOpenSearchClient loggingOpenSearchClient;
    private OpenSearchIndexFixture fixture;

    public FlattenedSearchingTests(OpenSearchSharedResource openSearchSharedResource) {
        this.loggingOpenSearchClient = openSearchSharedResource.getLoggingOpenSearchClient();
    }

    @BeforeEach
    public void setup() {
        fixture = new OpenSearchIndexFixture(loggingOpenSearchClient.getClient(), loggingOpenSearchClient.getLogger());
    }

    /**
     * Demonstrates that a single flattened field can be searched using dotted notation
     * (flattenedField.property). This shows how to search on individual properties within
     * a flattened object.
     * 
     * This test uses the flatObject field type to demonstrate flattened field functionality.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_SingleField_CanSearchByDottedNotation() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with attributes containing multiple properties using strongly-typed records
            // ProductAttribute: (color, size)
            ProductWithFlattenedAttribute[] products = new ProductWithFlattenedAttribute[]{
                    new ProductWithFlattenedAttribute("1", "Product1", new ProductAttribute("red", "large")),
                    new ProductWithFlattenedAttribute("2", "Product2", new ProductAttribute("blue", "medium")),
                    new ProductWithFlattenedAttribute("3", "Product3", new ProductAttribute("red", "small"))
            };
            testIndex.indexDocuments(products);

            // Search by attribute.color using term query
            SearchResponse<ProductWithFlattenedAttribute> result = loggingOpenSearchClient.search(s -> s
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
            SearchResponse<ProductWithFlattenedAttribute> sizeResult = loggingOpenSearchClient.search(s -> s
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
            ProductWithFlattenedAttribute[] products = new ProductWithFlattenedAttribute[]{
                    new ProductWithFlattenedAttribute("1", "Product1", new ProductAttribute("red", "large")),
                    new ProductWithFlattenedAttribute("2", "Product2", new ProductAttribute("blue", "medium")),
                    new ProductWithFlattenedAttribute("3", "Product3", new ProductAttribute("red", "small")),
                    new ProductWithFlattenedAttribute("4", "Product4", new ProductAttribute("red", "large"))
            };
            testIndex.indexDocuments(products);

            // Search for products where attribute.color="red" AND attribute.size="large"
            // This should work because we're matching multiple properties from the same flattened object
            SearchResponse<ProductWithFlattenedAttribute> result = loggingOpenSearchClient.search(s -> s
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
            SearchResponse<ProductWithFlattenedAttribute> smallResult = loggingOpenSearchClient.search(s -> s
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
     * Demonstrates that multiple flattened fields can match multiple values
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

            // Create documents with both primary and secondary attributes using different DTO types
            // Primary uses ProductAttribute: (color, size)
            // Secondary uses ProductSpecification: (brand, category) - different DTO demonstrates flexibility
            ProductWithTwoFlattenedAttributes[] products = new ProductWithTwoFlattenedAttributes[]{
                    new ProductWithTwoFlattenedAttributes("1", "Product1", 
                            new ProductAttribute("red", "large"),
                            new ProductSpecification("BrandA", "Electronics")),
                    new ProductWithTwoFlattenedAttributes("2", "Product2",
                            new ProductAttribute("red", "medium"),
                            new ProductSpecification("BrandB", "Clothing")),
                    new ProductWithTwoFlattenedAttributes("3", "Product3",
                            new ProductAttribute("blue", "small"),
                            new ProductSpecification("BrandA", "Electronics"))
            };
            testIndex.indexDocuments(products);

            // Search for products with primaryAttribute.color="red" AND secondaryAttribute.brand="BrandA"
            // This should work because we're matching across different flattened fields with different DTO types
            SearchResponse<ProductWithTwoFlattenedAttributes> result = loggingOpenSearchClient.search(s -> s
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
                                                            .field("secondaryAttribute.brand")
                                                            .value(FieldValue.of("BrandA"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithTwoFlattenedAttributes.class
            );

            // With flatObject type, matching across multiple flattened fields works correctly
            // Note: primaryAttribute uses ProductAttribute (color, size) while secondaryAttribute
            // uses ProductSpecification (brand, category) - demonstrating different DTO types work
            assertThat(result.hits().total().value()).isEqualTo(1);
            assertThat(result.hits().hits().stream()
                    .map(h -> h.source().getId()))
                    .containsExactly("1"); // Product1 matches: primaryAttribute.color="red" AND secondaryAttribute.brand="BrandA"

            // Search for primaryAttribute.color="red" alone - should match Product1, Product2, and Product3
            SearchResponse<ProductWithTwoFlattenedAttributes> primaryResult = loggingOpenSearchClient.search(s -> s
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

            // Search for secondaryAttribute.category="Electronics" alone - should match Product1 and Product3
            SearchResponse<ProductWithTwoFlattenedAttributes> secondaryResult = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("secondaryAttribute.category")
                                            .value(FieldValue.of("Electronics"))
                                    )
                            ),
                    ProductWithTwoFlattenedAttributes.class
            );

            assertThat(secondaryResult.hits().total().value()).isEqualTo(2);
            assertThat(secondaryResult.hits().hits().stream()
                    .map(h -> h.source().getId()))
                    .containsExactlyInAnyOrder("1", "3");
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
    public void flattenedMapping_NestedFlattenedField_CanSearchByNestedDottedNotation() throws Exception {
        // Create a test index with flattened mapping for the details field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("details", Property.of(p -> p.flatObject(f -> f))))) {

            // Create documents with nested attribute structures
            // ProductDetails: (attribute: ProductAttribute, description: String)
            // ProductAttribute: (color, size)
            ProductWithNestedFlattened[] products = new ProductWithNestedFlattened[]{
                    new ProductWithNestedFlattened("1", "Product1",
                            new ProductDetails(
                                    new ProductAttribute("red", "large"),
                                    "Premium quality product")),
                    new ProductWithNestedFlattened("2", "Product2",
                            new ProductDetails(
                                    new ProductAttribute("blue", "medium"),
                                    "Standard quality product")),
                    new ProductWithNestedFlattened("3", "Product3",
                            new ProductDetails(
                                    new ProductAttribute("red", "small"),
                                    "Compact design"))
            };
            testIndex.indexDocuments(products);

            // Search using multi-level dotted notation: details.attribute.color
            SearchResponse<ProductWithNestedFlattened> result = loggingOpenSearchClient.search(s -> s
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
            SearchResponse<ProductWithNestedFlattened> sizeResult = loggingOpenSearchClient.search(s -> s
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
            SearchResponse<ProductWithNestedFlattened> descriptionResult = loggingOpenSearchClient.search(s -> s
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
            SearchResponse<ProductWithNestedFlattened> combinedResult = loggingOpenSearchClient.search(s -> s
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

    /**
     * Demonstrates that flattened arrays can match multiple values from different objects
     * in the array, but cannot reliably match multiple values from the same object.
     * 
     * When you have an array like:
     * [
     *   {color: "red", size: "large"},
     *   {color: "blue", size: "small"}
     * ]
     * 
     * Searching for color="red" AND size="small" WILL match because both values exist
     * in the array (even though they're from different objects). This works because
     * flattened fields don't preserve object boundaries - they treat all values
     * across all array objects as a flat collection.
     * 
     * However, for accurate matching of multiple properties from the same array object,
     * you need nested or join field types.
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
            attributes.add(new ProductAttribute("red", "large"));  // Object 1: red and large
            attributes.add(new ProductAttribute("blue", "small"));   // Object 2: blue and small

            ProductWithFlattenedArray[] products = new ProductWithFlattenedArray[]{
                    new ProductWithFlattenedArray("1", "Product1", attributes)
            };
            testIndex.indexDocuments(products);

            // Search for color="red" AND size="small" from DIFFERENT objects
            // This WILL match because flattened fields treat arrays as flat collections
            // - attributes.color="red" matches (from first object)
            // - attributes.size="small" matches (from second object)
            // Both values exist in the array, so the document matches even though they're from different objects
            SearchResponse<ProductWithFlattenedArray> differentObjectsResult = loggingOpenSearchClient.search(s -> s
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
                                                            .value(FieldValue.of("small"))
                                                    )
                                            )
                                    )
                            ),
                    ProductWithFlattenedArray.class
            );

            // This SHOULD match because both "red" and "small" exist in the array
            // (even though they're from different objects)
            assertThat(differentObjectsResult.hits().total().value()).isEqualTo(1);
            assertThat(differentObjectsResult.hits().hits().get(0).source().getId()).isEqualTo("1");
            
            // Verify that individual searches also work
            SearchResponse<ProductWithFlattenedArray> colorResult = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attributes.color")
                                            .value(FieldValue.of("red"))
                                    )
                            ),
                    ProductWithFlattenedArray.class
            );

            assertThat(colorResult.hits().total().value()).isEqualTo(1);
            
            // This demonstrates that flattened arrays don't preserve object boundaries:
            // - Matching values from DIFFERENT objects works (as shown above)
            // - But you cannot reliably ensure values come from the SAME object
            // For accurate matching of multiple properties from the same array object,
            // you must use nested or join field types
        }
    }

    /**
     * Demonstrates that flattened mapping does not analyze inputs - sub-properties are indexed
     * as UNANALYZED keywords, preserving exact values including punctuation and capitalization.
     * This proves that term vectors incorrectly show tokenization - flattened fields actually
     * store exact values without any analysis.
     * 
     * Key findings:
     * - Exact matches work: "Red-Metal!" matches "Red-Metal!" (preserved as-is)
     * - Token searches fail: "red" does NOT match "Red-Metal!" (proving no analysis/tokenization)
     * - Case matters: "red" does NOT match "Red-Metal!" (proving no normalization)
     * 
     * This disproves the misleading term vector output which suggests analysis/tokenization occurs.
     * Flattened mapping does NOT analyze inputs - values are stored exactly as provided.
     *
     * @throws Exception If an I/O error occurs
     */
    @Test
    public void flattenedMapping_DoesNotAnalyzeInputs() throws Exception {
        // Create a test index with flattened mapping for the attribute field
        try (OpenSearchTestIndex testIndex = fixture.createTestIndex(mapping ->
                mapping.properties("attribute", Property.of(p -> p.flatObject(f -> f))))) {

            // Create and index a product document with punctuation in the attribute
            // Flattened mapping does not analyze inputs, so these exact values are preserved as-is
            ProductWithFlattenedAttribute productDocument = new ProductWithFlattenedAttribute(
                    "1", "Product1", new ProductAttribute("Red-Metal!", "Extra-Large!"));
            testIndex.indexDocuments(new ProductWithFlattenedAttribute[]{productDocument});

            // Search for the EXACT term with punctuation - this SHOULD work because flattened
            // mapping does not analyze inputs, so values are preserved exactly as provided
            SearchResponse<ProductWithFlattenedAttribute> exactMatchSearch = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attribute.color")
                                            .value(FieldValue.of("Red-Metal!"))
                                    )
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // Verify that searching for the exact term with punctuation DOES work
            // Flattened fields preserve exact values, so "Red-Metal!" matches "Red-Metal!"
            assertThat(exactMatchSearch.hits().total().value()).isEqualTo(1);
            assertThat(exactMatchSearch.hits().hits().get(0).source().getId()).isEqualTo("1");

            // Similarly, searching for exact "Extra-Large!" should also work
            SearchResponse<ProductWithFlattenedAttribute> exactSizeSearch = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attribute.size")
                                            .value(FieldValue.of("Extra-Large!"))
                                    )
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // Verify that searching for exact term DOES work
            assertThat(exactSizeSearch.hits().total().value()).isEqualTo(1);
            assertThat(exactSizeSearch.hits().hits().get(0).source().getId()).isEqualTo("1");

            // Now disprove tokenization by searching for individual tokens from term vector output
            // Term vectors incorrectly show "Red-Metal!" as "red" and "metal", but searches prove this is wrong
            SearchResponse<ProductWithFlattenedAttribute> tokenRedSearch = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attribute.color")
                                            .value(FieldValue.of("red"))
                                    )
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // Searching for "red" should return NO results - proving no tokenization occurs
            // If tokenization happened, "red" would match "Red-Metal!", but it doesn't
            assertThat(tokenRedSearch.hits().total().value()).isEqualTo(0);
            assertThat(tokenRedSearch.hits().hits()).isEmpty();

            // Search for "metal" token - should also return NO results
            SearchResponse<ProductWithFlattenedAttribute> tokenMetalSearch = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attribute.color")
                                            .value(FieldValue.of("metal"))
                                    )
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // Searching for "metal" should return NO results - proving no tokenization
            assertThat(tokenMetalSearch.hits().total().value()).isEqualTo(0);
            assertThat(tokenMetalSearch.hits().hits()).isEmpty();

            // Search for "extra" token from size - should return NO results
            SearchResponse<ProductWithFlattenedAttribute> tokenExtraSearch = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attribute.size")
                                            .value(FieldValue.of("extra"))
                                    )
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // Searching for "extra" should return NO results - proving no tokenization
            assertThat(tokenExtraSearch.hits().total().value()).isEqualTo(0);
            assertThat(tokenExtraSearch.hits().hits()).isEmpty();

            // Search for "large" token from size - should return NO results
            SearchResponse<ProductWithFlattenedAttribute> tokenLargeSearch = loggingOpenSearchClient.search(s -> s
                            .index(testIndex.getName())
                            .query(q -> q
                                    .term(t -> t
                                            .field("attribute.size")
                                            .value(FieldValue.of("large"))
                                    )
                            ),
                    ProductWithFlattenedAttribute.class
            );

            // Searching for "large" should return NO results - proving no tokenization
            assertThat(tokenLargeSearch.hits().total().value()).isEqualTo(0);
            assertThat(tokenLargeSearch.hits().hits()).isEmpty();

            // This demonstrates that flattened fields are UNANALYZED keywords that preserve exact values.
            // Term vectors incorrectly show tokenization, but search behavior proves values are stored exactly.
            // You must search using the exact value as indexed, including punctuation and capitalization.
        }
    }

}


