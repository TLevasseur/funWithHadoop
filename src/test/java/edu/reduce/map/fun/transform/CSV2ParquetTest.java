package edu.reduce.map.fun.transform;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static edu.reduce.map.fun.transform.CSV2Parquet.loadSchema;

public class CSV2ParquetTest {

    @BeforeClass
    public static void setup(){
        org.apache.log4j.BasicConfigurator.configure();
    }
    @Test
    public void should_load_schema() throws IOException {
        Schema schema = loadSchema();
        Assert.assertEquals(16,schema.getFields().size());
    }
}