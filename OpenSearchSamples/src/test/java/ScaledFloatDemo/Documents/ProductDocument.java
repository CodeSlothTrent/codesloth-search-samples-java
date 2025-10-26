package ScaledFloatDemo.Documents;

import KeywordDemo.Documents.IDocumentWithId;
import java.util.Objects;

/**
 * A sample document that contains a scaled_float field for testing scaled_float field type functionality.
 * Scaled_float requires a scaling_factor and is represented as double in Java.
 */
public class ProductDocument implements IDocumentWithId {
    private String id;
    private String name;
    private double price;

    public ProductDocument() {
    }

    public ProductDocument(int id, String name, double price) {
        this.id = String.valueOf(id);
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.price = price;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProductDocument that = (ProductDocument) o;
        return Double.compare(that.price, price) == 0 &&
                Objects.equals(id, that.id) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, price);
    }

    @Override
    public String toString() {
        return "ProductDocument{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", price=" + price +
                '}';
    }
}

