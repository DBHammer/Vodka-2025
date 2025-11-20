package bean;

import java.sql.Timestamp;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Order implements Comparable<Order> {
    public Timestamp o_entry_d;
    public double ol_amount_sum;

    public Order(Timestamp o_entry_d) {
        this.o_entry_d = o_entry_d;
    }

    public Order(Timestamp o_entry_d, double ol_amount_sum) {
        this.o_entry_d = o_entry_d;
        this.ol_amount_sum = ol_amount_sum;
    }

    @Override
    public int compareTo(Order other) {
        if (other == null)
            return 1;
        return this.o_entry_d.compareTo(other.o_entry_d);
    }
    // @Override
    // public int compareTo(Order other) {
    // int cmp = this.o_entry_d.compareTo(other.o_entry_d);
    // if (cmp != 0)
    // return cmp;
    // return -Double.compare(this.ol_amount_sum, other.ol_amount_sum);
    // }

}