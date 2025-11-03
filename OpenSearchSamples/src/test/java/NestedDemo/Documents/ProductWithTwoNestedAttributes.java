package NestedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains multiple nested fields (primaryAttribute and secondaryAttribute).
 * This demonstrates that you can match multiple values from different nested fields
 * in the same document using boolean must queries.
 * 
 * Uses different DTO types for each nested field:
 * - primaryAttribute uses ProductAttribute (color, size)
 * - secondaryAttribute uses ProductSpecification (brand, category)
 * 
 * Nested fields can use any DTO type you want—the same type or different types.
 * Each field can have its own structure with whatever properties you need.
 */
public class ProductWithTwoNestedAttributes implements IDocumentWithId {
    private String id;
    private String name;
    private ProductAttribute primaryAttribute; // Nested field for primary attribute information (strongly-typed record)
    private ProductSpecification secondaryAttribute; // Nested field for secondary attribute information (different DTO type)

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithTwoNestedAttributes() {
    }

    /**
     * Creates a new ProductWithTwoNestedAttributes with the specified ID, name, primaryAttribute, and secondaryAttribute.
     *
     * @param id                  The product ID
     * @param name                The product name
     * @param primaryAttribute    The primary product attribute as a record (will be serialized as JSON object)
     * @param secondaryAttribute  The secondary product attribute as a record (will be serialized as JSON object)
     */
    public ProductWithTwoNestedAttributes(String id, String name, ProductAttribute primaryAttribute, ProductSpecification secondaryAttribute) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.primaryAttribute = primaryAttribute;
        this.secondaryAttribute = secondaryAttribute;
    }

    /**
     * Gets the product ID.
     *
     * @return The product ID
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * Sets the product ID.
     *
     * @param id The product ID
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the product name.
     *
     * @return The product name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the product name.
     *
     * @param name The product name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the primary product attribute.
     * This record property will be mapped as a nested field type in OpenSearch.
     * Properties can be accessed using nested queries with the path parameter.
     *
     * @return The primary product attribute as a record
     */
    public ProductAttribute getPrimaryAttribute() {
        return primaryAttribute;
    }

    /**
     * Sets the primary product attribute.
     *
     * @param primaryAttribute The primary product attribute as a record
     */
    public void setPrimaryAttribute(ProductAttribute primaryAttribute) {
        this.primaryAttribute = primaryAttribute;
    }

    /**
     * Gets the secondary product attribute.
     * This record property will be mapped as a nested field type in OpenSearch.
     * Properties can be accessed using nested queries with the path parameter.
     * Note: This uses a different DTO type (ProductSpecification) than primaryAttribute. Nested fields
     * can use any DTO type you want—the same type or different types.
     *
     * @return The secondary product attribute as a record
     */
    public ProductSpecification getSecondaryAttribute() {
        return secondaryAttribute;
    }

    /**
     * Sets the secondary product attribute.
     *
     * @param secondaryAttribute The secondary product attribute as a record
     */
    public void setSecondaryAttribute(ProductSpecification secondaryAttribute) {
        this.secondaryAttribute = secondaryAttribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithTwoNestedAttributes that = (ProductWithTwoNestedAttributes) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(primaryAttribute, that.primaryAttribute) &&
                Objects.equals(secondaryAttribute, that.secondaryAttribute);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, primaryAttribute, secondaryAttribute);
    }

    @Override
    public String toString() {
        return "ProductWithTwoNestedAttributes{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", primaryAttribute=" + primaryAttribute +
                ", secondaryAttribute=" + secondaryAttribute +
                '}';
    }
}

