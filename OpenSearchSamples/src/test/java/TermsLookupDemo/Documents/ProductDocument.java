package TermsLookupDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;

import java.util.Objects;

/**
 * Represents a product in the catalog with metadata that is invariant across users.
 * This document is stored in the product index and queried via terms lookup
 * to find products matching a user's personalized interactions.
 */
public class ProductDocument implements IDocumentWithId {
    private String id;
    private String title;
    private double price;
    private String description;

    public ProductDocument() {
    }

    public ProductDocument(int id, String title, double price, String description) {
        this.id = String.valueOf(id);
        this.title = Objects.requireNonNull(title, "title cannot be null");
        this.price = price;
        this.description = Objects.requireNonNull(description, "description cannot be null");
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(int id) {
        this.id = String.valueOf(id);
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductDocument that = (ProductDocument) o;
        return Double.compare(that.price, price) == 0 &&
               Objects.equals(id, that.id) &&
               Objects.equals(title, that.title) &&
               Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, title, price, description);
    }

    @Override
    public String toString() {
        return "ProductDocument{" +
                "id='" + id + '\'' +
                ", title='" + title + '\'' +
                ", price=" + price +
                ", description='" + description + '\'' +
                '}';
    }
}
