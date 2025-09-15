package org.apache.hadoop.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile.Writer;
import org.junit.Test;

public class MapFileIndexIntervalDefaultTest {

    @Test
    public void testIndexIntervalDefaultWhenNotSpecified() throws IOException {
        // 1. Create Configuration without setting io.map.index.interval
        Configuration conf = new Configuration();

        // 2. Compute expected value using Configuration API (128 is the default)
        long expectedInterval = conf.getInt("io.map.index.interval", 128);

        // 3. Prepare temporary directory for MapFile
        java.nio.file.Path tempDir = Files.createTempDirectory("mapfile-test");
        Path dirPath = new Path(tempDir.toUri().toString());

        // 4. Create MapFile.Writer with the Configuration
        Writer writer = null;
        try {
            writer = new Writer(conf,
                                dirPath,
                                Writer.keyClass(org.apache.hadoop.io.Text.class),
                                Writer.valueClass(org.apache.hadoop.io.Text.class));

            // 5. Retrieve actual index interval
            long actualInterval = writer.getIndexInterval();

            // 6. Assert the default is used
            assertEquals("Default index interval should be 128", expectedInterval, actualInterval);
        } finally {
            if (writer != null) {
                writer.close();
            }
            FileSystem.get(conf).delete(dirPath, true);
        }
    }
}