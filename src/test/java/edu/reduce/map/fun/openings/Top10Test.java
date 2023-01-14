package edu.reduce.map.fun.openings;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static edu.reduce.map.fun.openings.Top10.getTop10Openings;

public class Top10Test {

    Text a = new Text("a");
    Text a2 = new Text("a");
    Text b = new Text("b");
    Text c = new Text("c");

    @Test
    public void should_count_the_number_of_appearance() {
        Map<Text, Integer> values = Top10.getAllValues(Arrays.asList(a, a2, b, c));
        Assert.assertEquals(values.get(a), new Integer(2));
        Assert.assertEquals(values.get(a2), new Integer(2));
        Assert.assertEquals(values.get(b), new Integer(1));
        Assert.assertEquals(values.get(c), new Integer(1));
    }

    @Test
    public void should_order_by_order_of_appearance() {
        List<Text> getTop10Openings = getTop10Openings(Top10.getAllValues(Arrays.asList(a, b, a2, b, a, c)));
        Assert.assertEquals(getTop10Openings.get(0), a);
        Assert.assertEquals(getTop10Openings.get(1), b);
        Assert.assertEquals(getTop10Openings.get(2), c);
    }
}