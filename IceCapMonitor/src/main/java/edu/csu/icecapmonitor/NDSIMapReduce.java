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
import org.apache.hadoop.io.DoubleWritable;

import java.io.IOException;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;

public class NDSIMapReduce {
    public static double normalizeDifference(double bandFour, double bandSix){
       return ((bandFour - bandSix) / (bandFour + bandSix));
    }
    public static class NDSIMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final DoubleWritable ndsiValue = new DoubleWritable();
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
                        band4Data = tarInput.readAllBytes();
                    } else if (entryName.endsWith("T2_B6.TIF")) {
                        band6Data = tarInput.readAllBytes();
                    }
                    if (band4Data != null && band6Data != null) break;
                }
            }

            if (band4Data != null && band6Data != null) {
                BufferedImage band4Image = ImageIO.read(new ByteArrayInputStream(band4Data));
                BufferedImage band6Image = ImageIO.read(new ByteArrayInputStream(band6Data));

                if (band4Image != null && band6Image != null) {
                    Raster band4Raster = band4Image.getData();
                    Raster band6Raster = band6Image.getData();

                    int width = band4Raster.getWidth();
                    int height = band4Raster.getHeight();

                    for (int y = 0; y < height; y++) {
                        for (int x = 0; x < width; x++) {
                            double bandFour = band4Raster.getSampleDouble(x, y, 0);
                            double bandSix = band6Raster.getSampleDouble(x, y, 0);
                            double ndsi = normalizeDifference(bandFour, bandSix);

                            // Output NDSI value for each pixel or region
                            ndsiValue.set(ndsi);
                            context.write(new Text(tarFilePath.getName()), ndsiValue); // change this to double
                        }
                    }
                }
            }
        }
    }

    public static class NDSIReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Double> values, Context context)  throws IOException, InterruptedException {
            // reducer code here

            double counterOfNIDSIValues = 0;
            double totalOfNDSIValues = 0;

            for(Double val : values){
                counterOfNIDSIValues++;
                totalOfNDSIValues = totalOfNDSIValues + val;
            }
            
            double avergeNDSIValue = totalOfNDSIValues / counterOfNIDSIValues;

            context.write(new Text(key), new DoubleWritable(avergeNDSIValue));
        }
    }

}
