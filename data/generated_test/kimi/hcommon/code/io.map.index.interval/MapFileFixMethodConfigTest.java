package org.apache.hadoop.io;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MapFileFixMethodConfigTest {

    private FileSystem fs;
    private Path testDir;
    private Configuration conf;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration();
        fs = FileSystem.getLocal(conf);
        testDir = new Path("target/test-" + UUID.randomUUID());
        fs.mkdirs(testDir);
    }

    @After
    public void tearDown() throws IOException {
        if (fs != null && testDir != null) {
            fs.delete(testDir, true);
        }
    }

    @Test
    public void testFixMethodUsesConfigurationInterval() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        int configuredInterval = conf.getInt("io.map.index.interval", 128);
        assertEquals(128, configuredInterval);   // default value

        // Override for this test only
        int testInterval = 50;
        conf.setInt("io.map.index.interval", testInterval);

        // 2. Prepare the test conditions.
        // Create a MapFile with 100 records using a different interval
        Path mapDir = new Path(testDir, "testMap");
        MapFile.Writer writer = new MapFile.Writer(conf, mapDir,
                MapFile.Writer.keyClass(Text.class),
                MapFile.Writer.valueClass(LongWritable.class));
        try {
            for (int i = 1; i <= 100; i++) {
                // Ensure keys are in lexicographic order
                writer.append(new Text(String.format("key%03d", i)), new LongWritable(i));
            }
        } finally {
            writer.close();
        }

        // Simulate corruption: delete index file
        Path indexFile = new Path(mapDir, MapFile.INDEX_FILE_NAME);
        assertTrue(fs.exists(indexFile));
        fs.delete(indexFile, true);
        assertFalse(fs.exists(indexFile));

        // 3. Test code.
        long entries = MapFile.fix(fs, mapDir, Text.class, LongWritable.class, false, conf);

        // 4. Code after testing.
        assertEquals(100, entries);

        // Count index entries
        int indexEntryCount = 0;
        try (SequenceFile.Reader indexReader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(indexFile))) {
            WritableComparable<?> key = (WritableComparable<?>) ReflectionUtils
                    .newInstance(indexReader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils
                    .newInstance(indexReader.getValueClass(), conf);
            while (indexReader.next(key, value)) {
                indexEntryCount++;
            }
        }

        // Expected = 100 / 50 = 2
        long expectedEntries = 100 / testInterval;
        assertEquals(expectedEntries, indexEntryCount);
    }
}