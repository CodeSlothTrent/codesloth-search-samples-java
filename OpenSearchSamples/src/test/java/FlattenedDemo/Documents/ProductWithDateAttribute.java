package FlattenedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains a flattened field for testing date range queries on flattened fields.
 * The flattened field represents a date attribute as a strongly-typed record.
 * This demonstrates range queries on flattened field date properties using dotted notation (e.g., attribute.createdDate).
 * 
 * The DateAttribute record is automatically serialized to JSON by the OpenSearch client.
 * Dates are stored as Strings to allow testing both ISO8601 formatted dates (which work with range queries)
 * and non-ISO formatted dates (which fail with range queries).
 */
public class ProductWithDateAttribute implements IDocumentWithId {
    private String id;
    private String name;
    private DateAttribute attribute; // Strongly-typed record that will be serialized as JSON object

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithDateAttribute() {
    }

    /**
     * Creates a new ProductWithDateAttribute with the specified ID, name, and date attribute.
     *
     * @param id        The product ID
     * @param name      The product name
     * @param attribute The date attribute as a record (will be serialized as JSON object)
     */
    public ProductWithDateAttribute(String id, String name, DateAttribute attribute) {
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
     * Gets the date attribute.
     * This record property will be mapped as a flattened field type in OpenSearch.
     * Properties can be accessed using dotted notation: attribute.createdDate, etc.
     *
     * @return The date attribute as a record
     */
    public DateAttribute getAttribute() {
        return attribute;
    }

    /**
     * Sets the date attribute.
     *
     * @param attribute The date attribute as a record
     */
    public void setAttribute(DateAttribute attribute) {
        this.attribute = attribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithDateAttribute that = (ProductWithDateAttribute) o;
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
        return "ProductWithDateAttribute{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", attribute=" + attribute +
                '}';
    }
}

