package FlattenedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains a flattened field for testing numeric range queries on flattened fields.
 * The flattened field represents a numeric attribute as a strongly-typed record.
 * This demonstrates range queries on flattened field numeric properties using dotted notation (e.g., attribute.value).
 * 
 * The NumericAttribute record is automatically serialized to JSON by the OpenSearch client.
 * Numeric values are stored as Strings to allow testing both padded and unpadded numeric values.
 * Since flattened fields store values as keywords, numeric values are compared lexicographically (ASCII),
 * not numerically. This requires zero-padding for range queries to work correctly.
 */
public class ProductWithNumericAttribute implements IDocumentWithId {
    private String id;
    private String name;
    private NumericAttribute attribute; // Strongly-typed record that will be serialized as JSON object

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithNumericAttribute() {
    }

    /**
     * Creates a new ProductWithNumericAttribute with the specified ID, name, and numeric attribute.
     *
     * @param id        The product ID
     * @param name      The product name
     * @param attribute The numeric attribute as a record (will be serialized as JSON object)
     */
    public ProductWithNumericAttribute(String id, String name, NumericAttribute attribute) {
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
     * Gets the numeric attribute.
     * This record property will be mapped as a flattened field type in OpenSearch.
     * Properties can be accessed using dotted notation: attribute.value, etc.
     *
     * @return The numeric attribute as a record
     */
    public NumericAttribute getAttribute() {
        return attribute;
    }

    /**
     * Sets the numeric attribute.
     *
     * @param attribute The numeric attribute as a record
     */
    public void setAttribute(NumericAttribute attribute) {
        this.attribute = attribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithNumericAttribute that = (ProductWithNumericAttribute) o;
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
        return "ProductWithNumericAttribute{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", attribute=" + attribute +
                '}';
    }
}
