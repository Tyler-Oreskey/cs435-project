package edu.csu.icecapmonitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class IceCapMonitorMapReduce extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new IceCapMonitorMapReduce(), args);
        System.exit(res);
    }

    public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
        // add map reduce job inside here
        return 0;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        if (args.length != 2) {
            System.err.println("Usage: IceCapMonitorMapReduce <input path> <output path>");
            System.exit(1);
        }
        return runJob(conf, args[0], args[1]);
    }
}