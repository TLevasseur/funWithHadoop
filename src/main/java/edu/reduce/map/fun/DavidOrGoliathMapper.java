package edu.reduce.map.fun;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DavidOrGoliathMapper extends Mapper<LongWritable, GameWritable, Text, IntWritable> {

    public static final Text GOLIATH = new Text("Goliath");
    public static final Text DAVID = new Text("David");

    private static final Text BLACK = new Text("black");
    private static final Text WHITE = new Text("white");

    @Override
    protected void map(LongWritable key, GameWritable value, Context context) throws IOException, InterruptedException {
        if (!GameWritables.EMPTY.equals(value)) {
            context.write(davidOrGoliath(value), new IntWritable(1));
        }
    }

    private static Text davidOrGoliath(GameWritable value) {
        if (goliathWon(value)) {
            return GOLIATH;
        } else {
            //Broke the odd even in draw !
            return DAVID;
        }
    }

    protected static boolean goliathWon(GameWritable value) {
        return (value.getBlackRating() > value.getWhiteRating() && BLACK.equals(value.getWinner()))
                || (value.getBlackRating() < value.getWhiteRating() && WHITE.equals(value.getWinner()));
    }
}
