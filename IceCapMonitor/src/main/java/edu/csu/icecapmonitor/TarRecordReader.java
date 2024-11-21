package edu.csu.icecapmonitor;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;

public class TarRecordReader extends RecordReader<LongWritable, Text> {
    private LongWritable currentKey;
    private Text currentValue;
    private Path current;
    private Boolean returned;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) split;
        current = fileSplit.getPath();
        returned = false;

        currentKey = new LongWritable();
        currentValue = new Text();
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
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
