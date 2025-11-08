package FlattenedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Map;
import java.util.Objects;

/**
 * A sample document that contains a flattened field with many numeric properties for extreme testing.
 * This uses a Map to allow dynamic property creation (e.g., 500000 distinct properties).
 * The flattened field represents numeric attributes as key-value pairs where values are zero-padded strings.
 */
public class ProductWithLargeFlattenedNumeric implements IDocumentWithId {
    private String id;
    private String name;
    private Map<String, String> numericAttributes; // Map of property names to zero-padded numeric values

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithLargeFlattenedNumeric() {
    }

    /**
     * Creates a new ProductWithLargeFlattenedNumeric with the specified ID, name, and numeric attributes.
     *
     * @param id                The product ID
     * @param name              The product name
     * @param numericAttributes Map of property names to zero-padded numeric value strings
     */
    public ProductWithLargeFlattenedNumeric(String id, String name, Map<String, String> numericAttributes) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.numericAttributes = numericAttributes;
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
     * Gets the numeric attributes map.
     * This map will be mapped as a flattened field type in OpenSearch.
     * Properties can be accessed using dotted notation: numericAttributes.propertyName, etc.
     *
     * @return The numeric attributes map
     */
    public Map<String, String> getNumericAttributes() {
        return numericAttributes;
    }

    /**
     * Sets the numeric attributes map.
     *
     * @param numericAttributes The numeric attributes map
     */
    public void setNumericAttributes(Map<String, String> numericAttributes) {
        this.numericAttributes = numericAttributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithLargeFlattenedNumeric that = (ProductWithLargeFlattenedNumeric) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(numericAttributes, that.numericAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, numericAttributes);
    }

    @Override
    public String toString() {
        return "ProductWithLargeFlattenedNumeric{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", numericAttributes.size=" + (numericAttributes != null ? numericAttributes.size() : 0) +
                '}';
    }
}


