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
        Job ndsi_job = Job.getInstance(conf, "Normalized Difference Snow Index Job");
        ndsi_job.setJarByClass(NDSIMapReduce.class);
        ndsi_job.setMapperClass(NDSIMapReduce.NDSIMapper.class);
        ndsi_job.setMapOutputKeyClass(Text.class);
        ndsi_job.setMapOutputValueClass(DoubleWritable.class);
        ndsi_job.setOutputKeyClass(Text.class);
        ndsi_job.setOutputValueClass(Text.class);
        ndsi_job.setInputFormatClass(TarInputFormat.class);
        ndsi_job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(ndsi_job, new Path(inputDir));
        FileOutputFormat.setOutputPath(ndsi_job, new Path(outputDir));

        return ndsi_job.waitForCompletion(true) ? 0 : 1;
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