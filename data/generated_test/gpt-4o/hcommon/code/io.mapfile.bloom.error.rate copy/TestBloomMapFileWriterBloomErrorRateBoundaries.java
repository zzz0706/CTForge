package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.*;

public class TestBloomMapFileWriterBloomErrorRateBoundaries {
    private Path testDir;
    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.getLocal(conf);
        testDir = new Path(System.getProperty("test.build.data", "target/test-data") +
                "/bloommapfile-" + UUID.randomUUID());
        fs.mkdirs(testDir);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null && testDir != null) {
            fs.delete(testDir, true);
        }
    }

    @Test
    public void testBloomMapFileWriterReactsToBloomErrorRateBoundaryValues() throws IOException {
        Configuration conf = new Configuration();
        Path bloomFilePathLow = new Path(testDir, "bloom-low");
        Path bloomFilePathHigh = new Path(testDir, "bloom-high");

        // BloomMapFile expects the error rate property name "io.mapfile.bloom.error.rate"
        final String BLOOM_ERROR_RATE_KEY = "io.mapfile.bloom.error.rate";
        // Set explicit error rates at boundary (very low and very high, but legal)
        float errorRateLow = 1e-8f;
        float errorRateHigh = 0.1f;

        conf.setFloat(BLOOM_ERROR_RATE_KEY, errorRateLow);

        // Prepare keys in lexicographically sorted order to satisfy MapFile requirements
        Text[] sortedLowKeys = new Text[100];
        for (int i = 0; i < 100; i++) {
            // This ensures that k00, k01, ... k99, so the order is correct
            sortedLowKeys[i] = new Text(String.format("low-error-%03d", i));
        }

        // Test: Low error rate boundary
        BloomMapFile.Writer writerLow = null;
        try {
            writerLow = new BloomMapFile.Writer(
                    conf,
                    FileSystem.getLocal(conf),
                    bloomFilePathLow.toString(),
                    Text.class,
                    BytesWritable.class
            );
            for (int i = 0; i < sortedLowKeys.length; i++) {
                BytesWritable value = new BytesWritable(("val-" + i).getBytes("UTF-8"));
                writerLow.append(sortedLowKeys[i], value);
            }
        } finally {
            if (writerLow != null) {
                writerLow.close();
            }
        }

        // Verify: Read back and check membership of a subset of added keys
        BloomMapFile.Reader readerLow = null;
        try {
            readerLow = new BloomMapFile.Reader(
                    FileSystem.getLocal(conf),
                    bloomFilePathLow.toString(),
                    conf
            );
            for (int i = 0; i < 10; i++) {
                Text key = new Text(String.format("low-error-%03d", i));
                BytesWritable value = new BytesWritable();
                assertTrue("Low error rate: key should exist in map file",
                        readerLow.get(key, value) != null);
            }
        } finally {
            if (readerLow != null) {
                readerLow.close();
            }
        }

        // Test: High error rate boundary
        Configuration confHigh = new Configuration();
        confHigh.setFloat(BLOOM_ERROR_RATE_KEY, errorRateHigh);

        Text[] sortedHighKeys = new Text[100];
        for (int i = 0; i < 100; i++) {
            sortedHighKeys[i] = new Text(String.format("high-error-%03d", i));
        }

        BloomMapFile.Writer writerHigh = null;
        try {
            writerHigh = new BloomMapFile.Writer(
                    confHigh,
                    FileSystem.getLocal(confHigh),
                    bloomFilePathHigh.toString(),
                    Text.class,
                    BytesWritable.class
            );
            for (int i = 0; i < sortedHighKeys.length; i++) {
                BytesWritable value = new BytesWritable(("val-" + i).getBytes("UTF-8"));
                writerHigh.append(sortedHighKeys[i], value);
            }
        } finally {
            if (writerHigh != null) {
                writerHigh.close();
            }
        }

        // Verify: Read back and check membership of a subset of added keys
        BloomMapFile.Reader readerHigh = null;
        try {
            readerHigh = new BloomMapFile.Reader(
                    FileSystem.getLocal(confHigh),
                    bloomFilePathHigh.toString(),
                    confHigh
            );
            for (int i = 0; i < 10; i++) {
                Text key = new Text(String.format("high-error-%03d", i));
                BytesWritable value = new BytesWritable();
                assertTrue("High error rate: key should exist in map file",
                        readerHigh.get(key, value) != null);
            }
        } finally {
            if (readerHigh != null) {
                readerHigh.close();
            }
        }
    }
}