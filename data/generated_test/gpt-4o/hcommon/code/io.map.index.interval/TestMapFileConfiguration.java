package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestMapFileConfiguration {

    // Ensure proper handling of the "fix" operation under different workloads by validating index rebuilding.
    @Test
    public void testFixOperationRebuildsIndexFromDataFile() throws Exception {
        // Step 1: Retrieve configuration values dynamically using the API
        Configuration conf = new Configuration();
        int indexInterval = conf.getInt("io.map.index.interval", 128);

        // Step 2: Prepare the input conditions for unit testing
        Path testDir = new Path("testMapFileDir");
        FileSystem fs = FileSystem.get(conf);

        // Cleanup before test to ensure directory is clean
        if (fs.exists(testDir)) {
            fs.delete(testDir, true);
        }
        fs.mkdirs(testDir);

        Path dataFile = new Path(testDir, "data");
        SequenceFile.Writer dataWriter = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(dataFile),
                SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(LongWritable.class));
        
        // Write sample data to the SequenceFile
        try {
            for (int i = 0; i < 1000; i++) {
                dataWriter.append(new LongWritable(i), new LongWritable(i));
            }
        } finally {
            dataWriter.close();
        }

        // Ensure index file does not exist before calling fix()
        Path indexFile = new Path(testDir, "index");
        if (fs.exists(indexFile)) {
            fs.delete(indexFile, false);
        }

        // Step 3: Call the fix method
        long recordCount = MapFile.fix(fs, testDir, LongWritable.class, LongWritable.class, false, conf);

        // Step 4: Validate that the fix method generates the index file correctly
        assertTrue("Index file should be created by fix method.", fs.exists(indexFile));

        // Step 5: Validate the index respects the configuration-defined index interval
        SequenceFile.Reader indexReader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(indexFile));

        try {
            LongWritable key = new LongWritable();
            LongWritable value = new LongWritable();
            long indexEntries = 0;

            while (indexReader.next(key, value)) {
                indexEntries++;
            }

            // Check that the number of entries approximately matches records divided by index interval
            long expectedIndexEntries = (1000 / indexInterval);
            assertTrue("Number of index entries should respect configuration interval.",
                    indexEntries >= expectedIndexEntries && indexEntries <= (expectedIndexEntries + 1));

        } finally {
            indexReader.close();
        }

        // Cleanup after test
        fs.delete(testDir, true);
    }
}