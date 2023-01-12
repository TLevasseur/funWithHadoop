package edu.reduce.map.fun;

import org.junit.Assert;
import org.junit.Test;

public class ChessRecordReaderTest {
    {
        org.apache.log4j.BasicConfigurator.configure();
    }

    @Test
    public void should_parse_line_correctly() {
        GameWritable gameWritable = ChessRecordReader.getGameWritableOrDefault("RVN0N3VK,FALSE,1.50368E+12,1.50368E+12,9,resign,black,15+30,daniel_likes_chess,1413,soultego,2108,e4 Nc6 d4 e5 d5 Nce7 c3 Ng6 b4,B00,Nimzowitsch Defense: Kennedy Variation |  Linksspringer Variation,5");
        Assert.assertEquals(2108,
                gameWritable.getBlackRating());
        Assert.assertEquals(1413,
                gameWritable.getWhiteRating());
        Assert.assertEquals("black",
                gameWritable.getWinner().toString());
    }

    @Test
    public void should_return_empty_when_string_is_not_correct() {
        Assert.assertEquals(GameWritables.EMPTY, ChessRecordReader.getGameWritableOrDefault("Totally a ,bad;line,n"));
    }
}