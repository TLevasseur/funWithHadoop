package edu.reduce.map.fun;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class GameWritables {
    public static final GameWritable EMPTY = new GameWritable();


    protected static GameWritable fromColumns(String[] columns) {
        return new GameWritable(
                parseText(columns, 0),
                parseBoolean(columns, 1),
                parseInt(columns, 4),
                parseText(columns, 5),
                parseText(columns, 6),
                parseText(columns, 7),
                parseText(columns, 8),
                parseInt(columns, 9),
                parseText(columns, 10),
                parseInt(columns, 11),
                parseText(columns, 12),
                parseText(columns, 13),
                parseText(columns, 14),
                parseInt(columns, 15)
        );
    }

    private static Text parseText(String[] columns, int x) {
        return new Text(columns[x]);
    }

    private static IntWritable parseInt(String[] columns, int x) {
        return new IntWritable(Integer.parseInt(columns[x]));
    }

    private static BooleanWritable parseBoolean(String[] columns, int x) {
        return new BooleanWritable(Boolean.parseBoolean(columns[x]));
    }


}
