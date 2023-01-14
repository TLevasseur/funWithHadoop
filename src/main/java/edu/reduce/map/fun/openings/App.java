package edu.reduce.map.fun.openings;

import edu.reduce.map.fun.ChessFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class App {

    public static void main(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        org.apache.log4j.BasicConfigurator.configure();
        final Job job = configureJob("Opening");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    private static Job configureJob(final String jobName) throws IOException {
        Job job = Job.getInstance(new Configuration(), jobName);
        job.setInputFormatClass(ChessFileInputFormat.class);
        job.setMapOutputKeyClass(LevelWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(OpeningByLevel.class);
        job.setPartitionerClass(LevelPartitioner.class);
        job.setReducerClass(Top10.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(OpeningByLevel.class);
        job.setNumReduceTasks(Level.values().length);
        return job;
    }
}
