package edu.reduce.map.fun.openings;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Top10 extends Reducer<LevelWritable, Text, Text, IntWritable> {

    private MultipleOutputs<Text, IntWritable> mos;

    @Override
    public void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(LevelWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Text, Integer> allValues = getAllValues(values);
        List<Text> top10Openings = getTop10Openings(allValues);
        for (int i = 0; i < top10Openings.size(); i++) {
            mos.write(top10Openings.get(i), new IntWritable(i + 1), key.getLevel().name());
        }
    }

    static List<Text> getTop10Openings(Map<Text, Integer> allValues) {
        return allValues.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(10).map(Map.Entry::getKey).collect(Collectors.toList());
    }

    protected static Map<Text, Integer> getAllValues(Iterable<Text> values) {
        Map<Text, Integer> allValues = new HashMap<>();
        for (Text v : values) {
            if (allValues.containsKey(v)) {
                allValues.put(v, allValues.get(v) + 1);
            } else {
                allValues.put(new Text(v), 1);
            }
        }
        return allValues;
    }
}
