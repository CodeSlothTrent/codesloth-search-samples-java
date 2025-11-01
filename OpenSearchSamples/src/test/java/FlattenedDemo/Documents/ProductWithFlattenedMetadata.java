package FlattenedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains a flattened field for testing flattened field type functionality.
 * The flattened field represents product metadata (title, description, tags) as a strongly-typed record.
 * This demonstrates searching on flattened field properties using dotted notation (e.g., metadata.title).
 * 
 * The ProductMetadata record is automatically serialized to JSON by the OpenSearch client.
 */
public class ProductWithFlattenedMetadata implements IDocumentWithId {
    private String id;
    private String name;
    private ProductMetadata metadata; // Strongly-typed record that will be serialized as JSON object

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithFlattenedMetadata() {
    }

    /**
     * Creates a new ProductWithFlattenedMetadata with the specified ID, name, and metadata.
     *
     * @param id       The product ID
     * @param name     The product name
     * @param metadata The product metadata as a record (will be serialized as JSON object)
     */
    public ProductWithFlattenedMetadata(String id, String name, ProductMetadata metadata) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.metadata = metadata;
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
     * Gets the product metadata.
     * This record property will be mapped as a flattened field type in OpenSearch.
     * Properties can be accessed using dotted notation: metadata.title, metadata.description, etc.
     *
     * @return The product metadata as a record
     */
    public ProductMetadata getMetadata() {
        return metadata;
    }

    /**
     * Sets the product metadata.
     *
     * @param metadata The product metadata as a record
     */
    public void setMetadata(ProductMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithFlattenedMetadata that = (ProductWithFlattenedMetadata) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, metadata);
    }

    @Override
    public String toString() {
        return "ProductWithFlattenedMetadata{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", metadata=" + metadata +
                '}';
    }
}

