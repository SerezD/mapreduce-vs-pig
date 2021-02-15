package project;

import java.io.*;
import org.apache.hadoop.io.*;

/*
 * ValuePair is a couple of type (Text, FloatWritable)
 * The BusinessMapper needs only the second field, for the average stars
 * The ReviewMapper puts the User_id in the first Field, the review (stars) in the second
 */

public class ValuePair implements WritableComparable<ValuePair> {

    private Text first;
    private FloatWritable second;

    public ValuePair() {
        set(new Text(), new FloatWritable());
    }

    public ValuePair(String first, float second) {
        set(new Text(first), new FloatWritable(second));
    }

    public ValuePair(String first, double second) {
        set(new Text(first), new FloatWritable((float) second));
    }

    public ValuePair(Text first, FloatWritable second) {
        set(first, second);
    }

    public void set(Text first, FloatWritable second) { this.first = first; this.second = second; }
    public Text getFirst() { return first; }
    public FloatWritable getSecond() { return second; }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int compareTo(ValuePair o) {
        int cmp = first.compareTo(o.first);
        if (cmp != 0){
            return cmp;
        }
        return second.compareTo(o.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ValuePair){
            ValuePair tmp = (ValuePair) o;
            return first.equals(tmp.first) && second.equals(tmp.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return first.toString() + "\t" + second.toString();
    }
}


