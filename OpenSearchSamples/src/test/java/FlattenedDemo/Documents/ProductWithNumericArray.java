package FlattenedDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.List;
import java.util.Objects;

/**
 * A sample document that contains a flattened field with an array of numeric values for extreme testing.
 * This allows testing 500000 values in a single flattened numeric field.
 * The flattened field represents numeric values as a list of zero-padded strings.
 */
public class ProductWithNumericArray implements IDocumentWithId {
    private String id;
    private String name;
    private List<String> numericValues; // List of zero-padded numeric value strings

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductWithNumericArray() {
    }

    /**
     * Creates a new ProductWithNumericArray with the specified ID, name, and numeric values.
     *
     * @param id            The product ID
     * @param name          The product name
     * @param numericValues List of zero-padded numeric value strings
     */
    public ProductWithNumericArray(String id, String name, List<String> numericValues) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.numericValues = numericValues;
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
     * Gets the numeric values list.
     * This list will be mapped as a flattened field type in OpenSearch.
     * Array elements can be accessed using array notation in queries.
     *
     * @return The numeric values list
     */
    public List<String> getNumericValues() {
        return numericValues;
    }

    /**
     * Sets the numeric values list.
     *
     * @param numericValues The numeric values list
     */
    public void setNumericValues(List<String> numericValues) {
        this.numericValues = numericValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductWithNumericArray that = (ProductWithNumericArray) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(numericValues, that.numericValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, numericValues);
    }

    @Override
    public String toString() {
        return "ProductWithNumericArray{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", numericValues.size=" + (numericValues != null ? numericValues.size() : 0) +
                '}';
    }
}


