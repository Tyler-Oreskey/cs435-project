package edu.csu.icecapmonitor;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TarInputFormat extends InputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new TarRecordReader();
    }

    @Override
    public List getSplits(JobContext context) throws IOException {
        List<FileSplit> splits = new ArrayList<>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path inputDir = new Path(context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir"));
        FileStatus[] fileStatuses = fs.listStatus(inputDir);

        // Add each tar file from input directory to input splits
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile() && fileStatus.getPath().toString().endsWith(".tar")) {
                splits.add(new FileSplit(fileStatus.getPath(), 0, fileStatus.getLen(), null));
            }
        }
        return splits;
    }
}
