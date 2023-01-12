package edu.reduce.map.fun.elo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableEloPair implements Writable {

    EloWritable left;
    EloWritable right;

    public WritableEloPair(EloWritable left, EloWritable right) {
        this.left = left;
        this.right = right;
    }

    public WritableEloPair() {
        left = new EloWritable();
        right = new EloWritable();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        left.write(out);
        right.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        left.readFields(in);
        right.readFields(in);
    }

    @Override
    public String toString() {
        return left.toString() + "\t" + right.toString();
    }
}
