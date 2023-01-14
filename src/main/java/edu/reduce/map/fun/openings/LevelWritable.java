package edu.reduce.map.fun.openings;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LevelWritable implements WritableComparable<LevelWritable> {

    private Level level;

    public LevelWritable(int i) {
        this.level = Level.from(i);
    }

    public LevelWritable() {

    }

    public Level getLevel() {
        return level;
    }

    @Override
    public int compareTo(LevelWritable o) {
        return level.compareTo(o.level);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new IntWritable(level.ordinal()).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        IntWritable ordinal = new IntWritable();
        ordinal.readFields(in);
        this.level = Level.values()[ordinal.get()];
    }
}
