package NestedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains a nested field with nested complex types.
 * This demonstrates multi-level nested structures where a nested field contains
 * another complex type that itself contains nested properties.
 * 
 * ProductDetails contains a ProductAttribute record, creating a nested nested structure.
 */
public class ProductWithNestedDetails implements IDocumentWithId {
    private String id;
    private String name;
    private ProductDetails details; // Nested field containing nested ProductAttribute

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithNestedDetails() {
    }

    /**
     * Creates a new ProductWithNestedDetails with the specified ID, name, and details.
     *
     * @param id       The product ID
     * @param name     The product name
     * @param details  The product details containing nested attributes (will be serialized as nested JSON object)
     */
    public ProductWithNestedDetails(String id, String name, ProductDetails details) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.details = details;
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
     * Gets the product details.
     * This record property will be mapped as a nested field type in OpenSearch.
     * Since ProductDetails contains a nested ProductAttribute, this creates a multi-level nested structure.
     *
     * @return The product details as a record
     */
    public ProductDetails getDetails() {
        return details;
    }

    /**
     * Sets the product details.
     *
     * @param details The product details as a record
     */
    public void setDetails(ProductDetails details) {
        this.details = details;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithNestedDetails that = (ProductWithNestedDetails) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(details, that.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, details);
    }

    @Override
    public String toString() {
        return "ProductWithNestedDetails{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", details=" + details +
                '}';
    }
}

