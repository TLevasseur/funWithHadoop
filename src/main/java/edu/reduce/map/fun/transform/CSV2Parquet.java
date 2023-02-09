package edu.reduce.map.fun.transform;

import edu.reduce.map.fun.ChessFileInputFormat;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;


public class CSV2Parquet extends Configured implements Tool {

    private static final Logger log = Logger.getLogger(CSV2Parquet.class);


    @Override
    public int run(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Job job = Job.getInstance(getConf(), "Parquet Converter");
        job.setJarByClass(MapToRecord.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(ChessFileInputFormat.class);
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        job.setMapperClass(MapToRecord.class);
        Schema schema = loadSchema();
        log.info("Schema : " + new AvroSchemaConverter().convert(schema).toString());
        AvroParquetOutputFormat.setOutputPath(job, new Path(args[1]));
        AvroParquetOutputFormat.setSchema(job, schema);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setCompressOutput(job, true);
        job.setNumReduceTasks(0);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static Schema loadSchema() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        return new Schema.Parser().parse(classloader.getResourceAsStream("schema/game.avsc"));
    }

    public static void main(String[] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        Configuration configuration = new Configuration();
        int res = ToolRunner.run(configuration, new CSV2Parquet(), args);
        System.exit(res);
    }
}
