package edu.csu.icecapmonitor;

import java.io.BufferedReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;

import java.io.IOException;


public class TarRecordReader extends RecordReader<LongWritable, Text> {

    private FSDataInputStream fileIn;
    private LongWritable currentKey;
    private Text currentValue;
    private Path current;
    private Boolean returned;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // Get the file from the split
        FileSplit fileSplit = (FileSplit) split;
        current = fileSplit.getPath();
        returned = false;

        currentKey = new LongWritable();
        currentValue = new Text();
        // FileSystem fs = filePath.getFileSystem(context.getConfiguration());

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!returned) {
            currentValue.set(current.toString());
            returned = true;
            return true;
        } else {
            return false;
        }
            
    }

    @Override
    public LongWritable getCurrentKey() {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
        // Progress can be determined based on the number of entries processed
        return 0; // Simple example, so we return 0 for now
    }

    @Override
    public void close() throws IOException {

    }
}
