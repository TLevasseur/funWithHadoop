package edu.reduce.map.fun.elo;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EloWritable implements Writable {
    Text id;
    IntWritable elo;

    public EloWritable(Text id, int elo) {
        this.id = id;
        this.elo = new IntWritable(elo);
    }

    public EloWritable() {
        this.id = new Text();
        this.elo = new IntWritable();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        elo.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        elo.readFields(in);
    }

    @Override
    public String toString() {
        return id + "\t" + elo;
    }
}
