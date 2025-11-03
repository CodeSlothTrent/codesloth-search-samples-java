package NestedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.List;
import java.util.Objects;

/**
 * A sample document that contains an array of nested objects.
 * This demonstrates the KEY ADVANTAGE of nested fields: matching multiple values
 * from a single object in the array - something that flattened arrays cannot do reliably.
 * 
 * Uses strongly-typed ProductAttribute records instead of generic Maps.
 */
public class ProductWithNestedArray implements IDocumentWithId {
    private String id;
    private String name;
    private List<ProductAttribute> attributes; // Array of strongly-typed records that will be serialized as JSON array

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithNestedArray() {
    }

    /**
     * Creates a new ProductWithNestedArray with the specified ID, name, and attributes.
     *
     * @param id         The product ID
     * @param name       The product name
     * @param attributes The product attributes as a List of records (will be serialized as JSON array of objects)
     */
    public ProductWithNestedArray(String id, String name, List<ProductAttribute> attributes) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.attributes = attributes;
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
     * Gets the product attributes array.
     * This array of records will be mapped as a nested field type in OpenSearch.
     * Nested fields preserve object boundaries, allowing you to match multiple properties
     * from the same array object - this is the key advantage over flattened arrays.
     *
     * @return The product attributes as a List of records
     */
    public List<ProductAttribute> getAttributes() {
        return attributes;
    }

    /**
     * Sets the product attributes.
     *
     * @param attributes The product attributes as a List of records
     */
    public void setAttributes(List<ProductAttribute> attributes) {
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithNestedArray that = (ProductWithNestedArray) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, attributes);
    }

    @Override
    public String toString() {
        return "ProductWithNestedArray{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", attributes=" + attributes +
                '}';
    }
}

