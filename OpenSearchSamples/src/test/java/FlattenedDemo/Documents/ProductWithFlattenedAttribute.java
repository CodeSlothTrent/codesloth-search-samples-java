package FlattenedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains a flattened field for testing flattened field type functionality.
 * The flattened field represents a single product attribute (color, size) as a strongly-typed record.
 * This demonstrates searching on flattened field properties using dotted notation (e.g., attribute.color).
 * 
 * The ProductAttribute record is automatically serialized to JSON by the OpenSearch client.
 */
public class ProductWithFlattenedAttribute implements IDocumentWithId {
    private String id;
    private String name;
    private ProductAttribute attribute; // Strongly-typed record that will be serialized as JSON object

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithFlattenedAttribute() {
    }

    /**
     * Creates a new ProductWithFlattenedAttribute with the specified ID, name, and attribute.
     *
     * @param id        The product ID
     * @param name      The product name
     * @param attribute The product attribute as a record (will be serialized as JSON object)
     */
    public ProductWithFlattenedAttribute(String id, String name, ProductAttribute attribute) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.attribute = attribute;
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
     * Gets the product attribute.
     * This record property will be mapped as a flattened field type in OpenSearch.
     * Properties can be accessed using dotted notation: attribute.color, attribute.size, etc.
     *
     * @return The product attribute as a record
     */
    public ProductAttribute getAttribute() {
        return attribute;
    }

    /**
     * Sets the product attribute.
     *
     * @param attribute The product attribute as a record
     */
    public void setAttribute(ProductAttribute attribute) {
        this.attribute = attribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithFlattenedAttribute that = (ProductWithFlattenedAttribute) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(attribute, that.attribute);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, attribute);
    }

    @Override
    public String toString() {
        return "ProductWithFlattenedAttribute{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", attribute=" + attribute +
                '}';
    }
}

