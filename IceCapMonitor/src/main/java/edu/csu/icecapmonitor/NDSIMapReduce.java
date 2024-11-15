package edu.csu.icecapmonitor;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.IOException;

public class NDSIMapReduce {
    public double normalizeDifference(double bandFour, double bandSix){
       return ((bandFour - bandSix) / (bandFour + bandSix));
    }
    public static class NDSIMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tarFilePathStr = value.toString();
            Path tarFilePath = new Path(tarFilePathStr);
            FileSystem fs = FileSystem.get(context.getConfiguration());

            byte[] band4Data = null;
            byte[] band6Data = null;

            try(FSDataInputStream fileIn = fs.open(tarFilePath);
                TarArchiveInputStream tarInput = new TarArchiveInputStream(fileIn)) {
                TarArchiveEntry entry;

                while ((entry = tarInput.getNextTarEntry()) != null) {
                    String entryName = entry.getName();
                    if (entryName.endsWith("T2_B4.TIF")) {
                        band4Data = new byte[(int) entry.getSize()];
                        tarInput.read(band4Data);
                    } else if (entryName.endsWith("T2_B6.TIF")) {
                        band6Data = new byte[(int) entry.getSize()];
                        tarInput.read(band6Data);
                    }
                    if (band4Data != null && band6Data != null) break;
                }
            }
            if (band4Data != null && band6Data != null) {
                // need to read band4 and band6 pixel data
            }
        }
    }

    public static class NDSIReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            // reducer code here
        }
    }

}
