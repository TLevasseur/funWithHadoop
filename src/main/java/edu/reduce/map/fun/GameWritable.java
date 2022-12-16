package edu.reduce.map.fun;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class GameWritable implements Writable {
    private Text id;
    private BooleanWritable rated;
    private IntWritable turns;
    private Text victory_status;
    private Text winner;
    private Text increment_code;
    private Text white_id;
    private IntWritable white_rating;
    private Text black_id;
    private IntWritable black_rating;
    private Text allMoves;
    private Text opening_eco;
    private Text opening_name;
    private IntWritable opening_ply;

    public int getBlackRating() {
        return black_rating.get();
    }

    public int getWhiteRating() {
        return white_rating.get();
    }

    public Text getWinner() {
        return winner;
    }

    public GameWritable(Text id, BooleanWritable rated, IntWritable turns, Text victory_status, Text winner, Text increment_code, Text white_id, IntWritable white_rating, Text black_id, IntWritable black_rating, Text AllMoves, Text opening_eco, Text opening_name, IntWritable opening_ply) {
        this.id = id;
        this.rated = rated;
        this.turns = turns;
        this.victory_status = victory_status;
        this.winner = winner;
        this.increment_code = increment_code;
        this.white_id = white_id;
        this.white_rating = white_rating;
        this.black_id = black_id;
        this.black_rating = black_rating;
        this.allMoves = AllMoves;
        this.opening_eco = opening_eco;
        this.opening_name = opening_name;
        this.opening_ply = opening_ply;
    }

    public GameWritable() {

    }


    public void write(DataOutput out) throws IOException {
        id.write(out);
        rated.write(out);
        turns.write(out);
        victory_status.write(out);
        winner.write(out);
        increment_code.write(out);
        white_id.write(out);
        white_rating.write(out);
        black_id.write(out);
        black_rating.write(out);
        allMoves.write(out);
        opening_eco.write(out);
        opening_name.write(out);
        opening_ply.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        rated.readFields(in);
        turns.readFields(in);
        victory_status.readFields(in);
        winner.readFields(in);
        increment_code.readFields(in);
        white_id.readFields(in);
        white_rating.readFields(in);
        black_id.readFields(in);
        black_rating.readFields(in);
        allMoves.readFields(in);
        opening_eco.readFields(in);
        opening_name.readFields(in);
        opening_ply.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GameWritable that = (GameWritable) o;
        return Objects.equals(id, that.id) && Objects.equals(rated, that.rated) && Objects.equals(turns, that.turns) && Objects.equals(victory_status, that.victory_status) && Objects.equals(winner, that.winner) && Objects.equals(increment_code, that.increment_code) && Objects.equals(white_id, that.white_id) && Objects.equals(white_rating, that.white_rating) && Objects.equals(black_id, that.black_id) && Objects.equals(black_rating, that.black_rating) && Objects.equals(allMoves, that.allMoves) && Objects.equals(opening_eco, that.opening_eco) && Objects.equals(opening_name, that.opening_name) && Objects.equals(opening_ply, that.opening_ply);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, rated, turns, victory_status, winner, increment_code, white_id, white_rating, black_id, black_rating, allMoves, opening_eco, opening_name, opening_ply);
    }
}
