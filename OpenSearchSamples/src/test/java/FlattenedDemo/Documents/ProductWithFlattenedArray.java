package FlattenedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.List;
import java.util.Objects;

/**
 * A sample document that contains an array of flattened objects.
 * This demonstrates the limitation that flattened arrays cannot match multiple values
 * from a single object in the array - you need to use nested or join types for that.
 * 
 * Uses strongly-typed ProductAttribute records instead of generic Maps.
 */
public class ProductWithFlattenedArray implements IDocumentWithId {
    private String id;
    private String name;
    private List<ProductAttribute> attributes; // Array of strongly-typed records that will be serialized as JSON array

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithFlattenedArray() {
    }

    /**
     * Creates a new ProductWithFlattenedArray with the specified ID, name, and attributes.
     *
     * @param id         The product ID
     * @param name       The product name
     * @param attributes The product attributes as a List of records (will be serialized as JSON array of objects)
     */
    public ProductWithFlattenedArray(String id, String name, List<ProductAttribute> attributes) {
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
     * This array of records will be mapped as a flattened field type in OpenSearch.
     * Note: Flattened arrays have limitations - you cannot match multiple values from a single object
     * in the array. For that, you need nested or join field types.
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
        ProductWithFlattenedArray that = (ProductWithFlattenedArray) o;
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
        return "ProductWithFlattenedArray{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", attributes=" + attributes +
                '}';
    }
}

