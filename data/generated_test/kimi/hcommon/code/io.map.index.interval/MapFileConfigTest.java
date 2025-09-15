package org.apache.hadoop.io;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

public class MapFileConfigTest {

    @Test
    public void testIndexEntryWrittenAtConfiguredInterval() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        // Allow test-resource overrides; do NOT call conf.set(...) here

        // 2. Dynamic Expected Value Calculation
        int indexInterval = conf.getInt("io.map.index.interval", 128);
        int totalRecords = 25;
        long expectedIndexEntries = (totalRecords - 1) / indexInterval + 1;

        // 3. Prepare the test conditions
        Path tmpDir = new Path(System.getProperty("java.io.tmpdir"),
                               "mapfile-test-" + UUID.randomUUID().toString());
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(tmpDir, true); // ensure clean start

        // 4. Test code
        MapFile.Writer writer = null;
        try {
            writer = new MapFile.Writer(conf, tmpDir,
                    MapFile.Writer.keyClass(Text.class),
                    MapFile.Writer.valueClass(LongWritable.class));

            for (int i = 0; i < totalRecords; i++) {
                writer.append(new Text("key" + String.format("%03d", i)),
                              new LongWritable(i));
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }

        // 5. Count actual index entries
        Path indexFile = new Path(tmpDir, "index");
        long actualIndexEntries = 0;
        Reader indexReader = null;
        try {
            indexReader = new Reader(conf, Reader.file(indexFile));
            Writable key = new Text();
            Writable value = new LongWritable();
            while (indexReader.next(key, value)) {
                actualIndexEntries++;
            }
        } finally {
            if (indexReader != null) {
                indexReader.close();
            }
        }

        // 6. Assertions and Verification
        assertEquals("Number of index entries should match expected",
                     expectedIndexEntries, actualIndexEntries);

        // cleanup
        fs.delete(tmpDir, true);
    }
}