package KeywordDemo.Documents;

import java.util.Arrays;
import java.util.Objects;

/**
 * A sample document that contains an array of keyword fields that is explored during testing.
 */
public class ProductDocumentWithMultipleNames implements IDocumentWithId {
    private String id;
    private String[] names;
    private int rank;

    /**
     * Default constructor for serialization/deserialization.
     */
    public ProductDocumentWithMultipleNames() {
    }

    /**
     * Creates a new ProductDocumentWithMultipleNames with the specified ID, names, and rank.
     *
     * @param id    The product ID
     * @param names The product names array
     * @param rank  The product rank
     */
    public ProductDocumentWithMultipleNames(int id, String[] names, int rank) {
        this.id = String.valueOf(id);
        this.names = Objects.requireNonNull(names, "names cannot be null");
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
     * Gets the product names.
     * This string array property will be mapped as keywords.
     *
     * @return The product names array
     */
    public String[] getNames() {
        return names;
    }

    /**
     * Sets the product names.
     *
     * @param names The product names array
     */
    public void setNames(String[] names) {
        this.names = names;
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
        ProductDocumentWithMultipleNames that = (ProductDocumentWithMultipleNames) o;
        return Objects.equals(id, that.id) && 
               Arrays.equals(names, that.names) &&
               rank == that.rank;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, rank);
        result = 31 * result + Arrays.hashCode(names);
        return result;
    }

    @Override
    public String toString() {
        return "ProductDocumentWithMultipleNames{" +
                "id=" + id +
                ", names=" + Arrays.toString(names) +
                ", rank=" + rank +
                '}';
    }
} 