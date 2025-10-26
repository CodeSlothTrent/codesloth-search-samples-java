package LongDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains a long field for testing long field type functionality.
 * The long field represents stock quantity, which can range from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.
 */
public class ProductDocument implements IDocumentWithId {
    private String id;
    private String name;
    private long stock;

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
     * @param stock The product stock quantity (long: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
     */
    public ProductDocument(int id, String name, long stock) {
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
     * This long property will be mapped as a long field type in OpenSearch.
     *
     * @return The product stock quantity
     */
    public long getStock() {
        return stock;
    }

    /**
     * Sets the product stock quantity.
     *
     * @param stock The product stock quantity
     */
    public void setStock(long stock) {
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

