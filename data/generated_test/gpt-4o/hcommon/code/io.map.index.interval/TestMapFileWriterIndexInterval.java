package org.apache.hadoop.io;
  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestMapFileWriterIndexInterval {

    public static class DummyKey implements WritableComparable<DummyKey> {
        public DummyKey() {
            // Default constructor required for instantiation
        }

        @Override
        public int compareTo(DummyKey o) {
            return 0;
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
        }
    }

    public static class DummyValue implements Writable {
        public DummyValue() {
            // Default constructor required for instantiation
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
        }
    }

    @Test
    public void testWriterConstructionWithIndexInterval() throws IOException {
        // Step 1: Load configuration using the API
        Configuration conf = new Configuration();
        int configuredIndexInterval = conf.getInt("io.map.index.interval", 128);

        // Step 2: Prepare testing environment
        Path tempDir = new Path(System.getProperty("java.io.tmpdir"), "mapfile-test");

        // Step 3: Instantiate the MapFile.Writer with proper options
        try (MapFile.Writer writer = new MapFile.Writer(conf, tempDir, 
                MapFile.Writer.keyClass(DummyKey.class), 
                MapFile.Writer.valueClass(DummyValue.class))) {
            writer.setIndexInterval(configuredIndexInterval);

            // Validate that the writer has been configured correctly
            int actualIndexInterval = writer.getIndexInterval();
            Assert.assertEquals("Index interval should match the configured value", configuredIndexInterval, actualIndexInterval);
        }

        // Cleanup resources after testing (automatic due to try-with-resources)
    }
}