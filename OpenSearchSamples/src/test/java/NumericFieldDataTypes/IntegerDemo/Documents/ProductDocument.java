package NumericFieldDataTypes.IntegerDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains an integer field for testing integer field type functionality.
 * The integer field represents stock quantity, which can range from -2,147,483,648 to 2,147,483,647.
 */
public class ProductDocument implements IDocumentWithId {
    private String id;
    private String name;
    private int stock;

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductDocument() {
    }

    /**
     * Creates a new ProductDocument with the specified ID, name, and stock.
     *
     * @param id    The product ID
     * @param name  The product name
     * @param stock The product stock quantity (integer: -2,147,483,648 to 2,147,483,647)
     */
    public ProductDocument(int id, String name, int stock) {
        this.id = String.valueOf(id);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.stock = stock;
    }

    /**
     * Gets the product ID.
     * The Id field of a document is automatically used for the document id at indexing time.
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
     * Gets the product stock quantity.
     * This integer property will be mapped as an integer field type in OpenSearch.
     *
     * @return The product stock quantity
     */
    public int getStock() {
        return stock;
    }

    /**
     * Sets the product stock quantity.
     *
     * @param stock The product stock quantity
     */
    public void setStock(int stock) {
        this.stock = stock;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductDocument that = (ProductDocument) o;
        return stock == that.stock &&
                Objects.equals(id, that.id) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, stock);
    }

    @Override
    public String toString() {
        return "ProductDocument{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", stock=" + stock +
                '}';
    }
}

