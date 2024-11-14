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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TarInputFormat extends InputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new TarRecordReader();
    }

    @Override
    public List getSplits(JobContext context) throws IOException {
        // In this simple example, each .tar file is treated as a single split.
        // In a more advanced implementation, you could split large .tar files into smaller pieces.
        List<FileSplit> splits = new ArrayList<>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path inputDir = new Path(context.getConfiguration().get("mapred.input.dir"));
        FileStatus[] fileStatuses = fs.listStatus(inputDir);

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile() && fileStatus.getPath().toString().endsWith(".tar")) {
                splits.add(new FileSplit(fileStatus.getPath(), 0, fileStatus.getLen(), null));
            }
        }
        return splits;
    }
}
