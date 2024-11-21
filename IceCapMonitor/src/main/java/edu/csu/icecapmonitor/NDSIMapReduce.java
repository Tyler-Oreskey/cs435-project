package edu.csu.icecapmonitor;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import java.io.IOException;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;

public class NDSIMapReduce {
    public static double normalizeDifference(double bandFour, double bandSix) {
        return ((bandFour - bandSix) / (bandFour + bandSix));
    }

    public static int categorizeNDSI(double ndsi) {
        if (ndsi < 0.2) {
            return 0; // Category 1
        } else if (ndsi >= 0.2 && ndsi < 0.4) {
            return 1; // Category 2
        } else if (ndsi >= 0.4 && ndsi < 0.6) {
            return 2; // Category 3
        } else if (ndsi >= 0.6 && ndsi < 0.8) {
            return 3; // Category 4
        } else {
            return 4; // Category 5
        }
    }

    public static class NDSIMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String tarFilePathStr = value.toString();
            Path tarFilePath = new Path(tarFilePathStr);
            FileSystem fs = FileSystem.get(context.getConfiguration());

            byte[] band4Data = null;
            byte[] band6Data = null;

            // Iterate through tar file and read data for Bands 4 and 6
            try (FSDataInputStream fileIn = fs.open(tarFilePath);
                    TarArchiveInputStream tarInput = new TarArchiveInputStream(fileIn)) {
                TarArchiveEntry entry;

                while ((entry = tarInput.getNextTarEntry()) != null) {
                    String entryName = entry.getName();
                    if (entryName.endsWith("T2_B4.TIF")) {
                        band4Data = tarInput.readAllBytes();
                    } else if (entryName.endsWith("T2_B6.TIF")) {
                        band6Data = tarInput.readAllBytes();
                    }
                    if (band4Data != null && band6Data != null)
                        break;
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

                    long[] categoryCounts = new long[5];

                    // Calculate NDSI for every pixel in the band files
                    for (int y = 0; y < height; y++) {
                        for (int x = 0; x < width; x++) {
                            double bandFour = band4Raster.getSampleDouble(x, y, 0);
                            double bandSix = band6Raster.getSampleDouble(x, y, 0);
                            double ndsi = normalizeDifference(bandFour, bandSix);

                            // Increment pixel count for category
                            categoryCounts[categorizeNDSI(ndsi)]++;
                        }
                    }

                    // Write pixel counts to context with category
                    for (int i = 0; i < categoryCounts.length; i++) {
                        context.write(new Text(tarFilePath.getName()), new Text(i + "," + categoryCounts[i]));
                    }
                }
            }
        }
    }
}
