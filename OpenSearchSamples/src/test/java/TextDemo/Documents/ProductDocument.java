package TextDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;

import java.util.Objects;

/**
 * A sample document that contains a single text field that is explored during multiple tests within the suite.
 */
public class ProductDocument implements IDocumentWithId {
    private String id;
    private String description;
    private int rank;

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductDocument() {
    }

    /**
     * Creates a new ProductDocument with the specified ID, description, and rank.
     *
     * @param id          The product ID
     * @param description The product description
     * @param rank        The product rank
     */
    public ProductDocument(int id, String description, int rank) {
        this.id = String.valueOf(id);
        this.description = Objects.requireNonNull(description, "description cannot be null");
        this.rank = rank;
    }

    /**
     * Gets the product ID.
     * The Id field of a document is automatically used for the document id at indexing time.
     *
     * @return The product ID
     */
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
     * Gets the product description.
     * The string property of this document will be mapped as Text.
     * Conceptually this property could represent a description of a product.
     *
     * @return The product description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the product description.
     *
     * @param description The product description
     */
    public void setDescription(String description) {
        this.description = description;
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
               Objects.equals(description, that.description) &&
               rank == that.rank;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, description, rank);
    }

    @Override
    public String toString() {
        return "ProductDocument{" +
                "id=" + id +
                ", description='" + description + '\'' +
                ", rank=" + rank +
                '}';
    }
} 