package edu.csu.icecapmonitor;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class NDSIMapReduce {
    public double normalizeDifference(double bandFour, double bandSix){
       return ((bandFour - bandSix) / (bandFour + bandSix));
    }
    public static class NDSIMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // mapper code here
        }
    }

    public static class NDSIReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            // reducer code here
        }
    }

}
