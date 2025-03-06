package KeywordDemo.Documents;

import java.util.Arrays;
import java.util.Objects;

/**
 * A document that represents a user's favorite products.
 * This is used for testing adjacency matrix aggregations.
 */
public class UserFavouriteProducts implements IDocumentWithId {
    private String id;
    private String[] productNames;

    /**
     * Default constructor for serialization/deserialization.
     */
    public UserFavouriteProducts() {
    }

    /**
     * Creates a new UserFavouriteProducts with the specified user ID and product names.
     *
     * @param userId       The user ID
     * @param productNames The array of product names
     */
    public UserFavouriteProducts(int userId, String[] productNames) {
        this.id = String.valueOf(userId);
        this.productNames = Objects.requireNonNull(productNames, "productNames cannot be null");
    }

    /**
     * Gets the user ID.
     * The Id field of a document is automatically used for the document id at indexing time.
     *
     * @return The user ID
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * Sets the user ID.
     *
     * @param userId The user ID
     */
    public void setId(int userId) {
        this.id = String.valueOf(userId);
    }

    /**
     * Gets the product names.
     * This string array property will be mapped as keywords.
     *
     * @return The array of product names
     */
    public String[] getProductNames() {
        return productNames;
    }

    /**
     * Sets the product names.
     *
     * @param productNames The array of product names
     */
    public void setProductNames(String[] productNames) {
        this.productNames = productNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserFavouriteProducts that = (UserFavouriteProducts) o;
        return Objects.equals(id, that.id) && Arrays.equals(productNames, that.productNames);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id);
        result = 31 * result + Arrays.hashCode(productNames);
        return result;
    }

    @Override
    public String toString() {
        return "UserFavouriteProducts{" +
                "id='" + id + '\'' +
                ", productNames=" + Arrays.toString(productNames) +
                '}';
    }
} 