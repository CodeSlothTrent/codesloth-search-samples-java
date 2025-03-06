package KeywordDemo.Documents;

import java.util.Objects;

/**
 * A sample document that contains a single keyword field that is explored during multiple tests within the suite.
 */
public class ProductDocument implements IDocumentWithId {
    private String id;
    private String name;
    private int rank;

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductDocument() {
    }

    /**
     * Creates a new ProductDocument with the specified ID and name.
     *
     * @param id   The product ID
     * @param name The product name
     * @param rank The product rank
     */
    public ProductDocument(int id, String name, int rank) {
        this.id = String.valueOf(id);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.rank = rank;
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
    public void setId(int id) {
        this.id = String.valueOf(id);
    }

    /**
     * Gets the product name.
     * This string property will be mapped as a keyword.
     * Conceptually this property may represent the name of a product.
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
     * Gets the product rank.
     *
     * @return The product rank
     */
    public int getRank() {
        return rank;
    }

    /**
     * Sets the product rank.
     *
     * @param rank The product rank
     */
    public void setRank(int rank) {
        this.rank = rank;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductDocument that = (ProductDocument) o;
        return Objects.equals(id, that.id) && 
               Objects.equals(name, that.name) &&
               rank == that.rank;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, rank);
    }

    @Override
    public String toString() {
        return "ProductDocument{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", rank=" + rank +
                '}';
    }
} 