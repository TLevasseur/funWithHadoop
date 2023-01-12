package edu.reduce.map.fun.david;

import edu.reduce.map.fun.GameWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static edu.reduce.map.fun.david.DavidOrGoliathMapper.goliathWon;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DavidOrGoliathMapperTest {

    @Test
    public void should_be_true_when_the_highest_elo_player_won() {
        assertTrue(goliathWon(gameOf(10, 12, "black")));
        assertTrue(goliathWon(gameOf(100, 12, "white")));
    }

    @Test
    public void should_be_false_otherwise() {
        assertFalse(goliathWon(gameOf(10, 12, "draw")));
        assertFalse(goliathWon(gameOf(10, 12, "white")));
        assertFalse(goliathWon(gameOf(12, 10, "black")));
    }

    private GameWritable gameOf(int whiteRating, int blackRating, String winner) {
        return new GameWritable(
                null,
                null,
                null,
                null,
                new Text(winner),
                null,
                null,
                new IntWritable(whiteRating),
                null,
                new IntWritable(blackRating),
                null,
                null,
                null,
                null
        );
    }
}