package edu.reduce.map.fun;

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import static edu.reduce.map.fun.transform.CSV2Parquet.loadSchema;

public class GameWritables {
    public static final GameWritable EMPTY = new GameWritable();


    public static Record record(GameWritable gameWritable) throws IOException {
        Record record = new Record(loadSchema());
        record.put("id", gameWritable.getGameId().toString());
        record.put("rated", gameWritable.getRated().get());
        record.put("turns", gameWritable.getTurns().get());
        record.put("victory_status", gameWritable.getVictory_status().toString());
        record.put("winner", gameWritable.getWinner().toString());
        record.put("increment_code", gameWritable.getIncrement_code().toString());
        record.put("white_id", gameWritable.getWhite_id().toString());
        record.put("white_rating", gameWritable.getWhiteRating());
        record.put("black_id", gameWritable.getBlack_id().toString());
        record.put("black_rating", gameWritable.getBlackRating());
        record.put("moves", gameWritable.getAllMoves().toString());
        record.put("opening_eco", gameWritable.getOpening_eco().toString());
        record.put("opening_name", gameWritable.getOpening().toString());
        record.put("opening_ply", gameWritable.getOpening_ply().get());
        return record;
    }

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
