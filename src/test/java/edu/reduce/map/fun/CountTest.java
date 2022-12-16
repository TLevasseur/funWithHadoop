package edu.reduce.map.fun;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import static edu.reduce.map.fun.Count.countHelper;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class CountTest {

    private final IntWritable i3 = new IntWritable(3);
    private final IntWritable i5 = new IntWritable(5);
    private final IntWritable i7 = new IntWritable(7);

    @Test
    public void should_sum_given_values() {
        assertEquals(15, countHelper(asList(i3, i5, i7)));
    }

    /**
     * Property Based Testing :
     * Having both associativity and commutativity enables us to also use the reducer as a combiner.
     */
    @Test
    public void should_be_associative() {
        assertEquals(
                countHelper(asList(i7, new IntWritable(countHelper(asList(i3, i5))))),
                countHelper(asList(new IntWritable(countHelper(asList(i7, i5))), i3)));
    }

    @Test
    public void should_be_commutative() {
        assertEquals(countHelper(asList(i3, i5)), countHelper(asList(i5, i3)));
    }
}