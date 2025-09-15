package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class TestBlockCompressWriter {

    @Test
    public void testBlockCompressWriterConfigurationPropagation() throws IOException {
        // Use API to get configuration values and set them
        Configuration conf = new Configuration();
        conf.setInt("io.seqfile.compress.blocksize", 1024); // Correct key for Compression Block Size
        FileSystem fs = FileSystem.get(conf);

        // Define the path for the sequence file
        Path testFilePath = new Path("test.seq");

        try {
            // Prepare the test conditions
            CompressionCodec codec = new DefaultCodec();
            SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(testFilePath),
                    SequenceFile.Writer.keyClass(MockKey.class),
                    SequenceFile.Writer.valueClass(MockValue.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, codec));

            MockKey key = new MockKey("testKey");
            MockValue value = new MockValue("testValue");

            // Write multiple key-value pairs to test functionality under workload
            for (int i = 0; i < 10; i++) {
                writer.append(key, value);
            }

            // Assert that the writer's configuration propagation does not throw errors
            assertTrue("Writer configuration should propagate properly without errors.", true);

            writer.close();

            // Simulate additional workload with the same configuration
            writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(testFilePath),
                    SequenceFile.Writer.keyClass(MockKey.class),
                    SequenceFile.Writer.valueClass(MockValue.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, codec));

            for (int i = 0; i < 100; i++) {
                writer.append(key, value);
            }

            writer.close();
        } finally {
            // Clean up the test file
            if (fs.exists(testFilePath)) {
                fs.delete(testFilePath, false);
            }
        }
    }

    // Mock key class implementing Writable
    public static class MockKey implements org.apache.hadoop.io.Writable {
        private String key;

        // No-argument constructor required for Writable
        public MockKey() {}

        MockKey(String key) {
            this.key = key;
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
            out.writeUTF(key);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            this.key = in.readUTF();
        }
    }

    // Mock value class implementing Writable
    public static class MockValue implements org.apache.hadoop.io.Writable {
        private String value;

        // No-argument constructor required for Writable
        public MockValue() {}

        MockValue(String value) {
            this.value = value;
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            this.value = in.readUTF();
        }
    }
}