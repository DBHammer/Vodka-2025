package bean;

import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;

@Getter
@Setter
public class OrderLine implements Comparable<OrderLine> {
    private Timestamp ol_delivery_d;

    public OrderLine(Timestamp ol_delivery_d) {
        this.ol_delivery_d = ol_delivery_d;
    }

    @Override
    public int compareTo(OrderLine other) {
        if (other == null) return 1;  
        return this.ol_delivery_d.compareTo(other.ol_delivery_d);
    }
}