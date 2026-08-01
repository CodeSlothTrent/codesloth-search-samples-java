package TermsLookupDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;

import java.util.Arrays;
import java.util.Objects;

/**
 * Stores a single user's personalized product interactions.
 * Each keyword array field contains product IDs representing a specific type of engagement,
 * enabling terms lookup queries to cross-reference against the product catalog index.
 */
public class UserPersonalizationDocument implements IDocumentWithId {
    private String id;
    private String[] favoritedProductIds;
    private String[] recentlyViewedProductIds;
    private String[] inCartProductIds;

    public UserPersonalizationDocument() {
    }

    public UserPersonalizationDocument(String userId, String[] favoritedProductIds,
                                       String[] recentlyViewedProductIds, String[] inCartProductIds) {
        this.id = Objects.requireNonNull(userId, "userId cannot be null");
        this.favoritedProductIds = favoritedProductIds != null ? favoritedProductIds : new String[0];
        this.recentlyViewedProductIds = recentlyViewedProductIds != null ? recentlyViewedProductIds : new String[0];
        this.inCartProductIds = inCartProductIds != null ? inCartProductIds : new String[0];
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String userId) {
        this.id = userId;
    }

    public String[] getFavoritedProductIds() {
        return favoritedProductIds;
    }

    public void setFavoritedProductIds(String[] favoritedProductIds) {
        this.favoritedProductIds = favoritedProductIds;
    }

    public String[] getRecentlyViewedProductIds() {
        return recentlyViewedProductIds;
    }

    public void setRecentlyViewedProductIds(String[] recentlyViewedProductIds) {
        this.recentlyViewedProductIds = recentlyViewedProductIds;
    }

    public String[] getInCartProductIds() {
        return inCartProductIds;
    }

    public void setInCartProductIds(String[] inCartProductIds) {
        this.inCartProductIds = inCartProductIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserPersonalizationDocument that = (UserPersonalizationDocument) o;
        return Objects.equals(id, that.id) &&
               Arrays.equals(favoritedProductIds, that.favoritedProductIds) &&
               Arrays.equals(recentlyViewedProductIds, that.recentlyViewedProductIds) &&
               Arrays.equals(inCartProductIds, that.inCartProductIds);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id);
        result = 31 * result + Arrays.hashCode(favoritedProductIds);
        result = 31 * result + Arrays.hashCode(recentlyViewedProductIds);
        result = 31 * result + Arrays.hashCode(inCartProductIds);
        return result;
    }

    @Override
    public String toString() {
        return "UserPersonalizationDocument{" +
                "id='" + id + '\'' +
                ", favoritedProductIds=" + Arrays.toString(favoritedProductIds) +
                ", recentlyViewedProductIds=" + Arrays.toString(recentlyViewedProductIds) +
                ", inCartProductIds=" + Arrays.toString(inCartProductIds) +
                '}';
    }
}
