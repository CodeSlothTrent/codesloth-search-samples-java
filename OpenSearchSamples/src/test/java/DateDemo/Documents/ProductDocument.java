package DateDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains a date field for testing date field type functionality.
 * The date field represents product creation or last updated timestamp.
 */
public class ProductDocument implements IDocumentWithId {
    private String id;
    private String name;
    private String createdAt; // Date field for product creation timestamp (stored as ISO-8601 string)

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductDocument() {
    }

    /**
     * Creates a new ProductDocument with the specified ID, name, and creation date.
     *
     * @param id        The product ID
     * @param name      The product name
     * @param createdAt The product creation timestamp as ISO-8601 string (e.g., "2024-01-15T10:30:00Z")
     */
    public ProductDocument(int id, String name, String createdAt) {
        this.id = String.valueOf(id);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.createdAt = createdAt;
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
     * Gets the product creation timestamp.
     * This String property will be mapped as a date field type in OpenSearch.
     * Dates are stored as ISO-8601 strings (e.g., "2024-01-15T10:30:00Z").
     *
     * @return The product creation timestamp
     */
    public String getCreatedAt() {
        return createdAt;
    }

    /**
     * Sets the product creation timestamp.
     *
     * @param createdAt The product creation timestamp as ISO-8601 string
     */
    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductDocument that = (ProductDocument) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(createdAt, that.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, createdAt);
    }

    @Override
    public String toString() {
        return "ProductDocument{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }
}

