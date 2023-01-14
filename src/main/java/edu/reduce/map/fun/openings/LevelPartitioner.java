package edu.reduce.map.fun.openings;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LevelPartitioner extends Partitioner<LevelWritable, Text> {
    @Override
    public int getPartition(LevelWritable key, Text value, int numPartitions) {
        return key.getLevel().ordinal();
    }

}
