package edu.csu.icecapmonitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class IceCapMonitorMapReduce extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new IceCapMonitorMapReduce(), args);
        System.exit(res);
    }

    public static int runJob(Configuration conf, String inputDir, String outputDir) throws Exception {
        Job job1 = Job.getInstance(conf, "Normalized Difference Snow Index Job");
        job1.setJarByClass(NDSIMapReduce.class);
        job1.setMapperClass(NDSIMapReduce.NDSIMapper.class);
        job1.setReducerClass(NDSIMapReduce.NDSIReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setInputFormatClass(TarInputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(inputDir));
        FileOutputFormat.setOutputPath(job1, new Path(outputDir));

        return job1.waitForCompletion(true) ? 0 : 1;
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