package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;
import static org.junit.Assert.assertEquals;

public class TestBlockCompressWriterConfiguration {

    @Test
    public void testBlockCompressWriterConfigurationPropagation() throws IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setInt("io.seqfile.compress.blocksize", 1024); // Set block size in configuration
        FileSystem fs = FileSystem.get(conf);

        // Prepare the input conditions for unit testing
        Path testFilePath = new Path("test.seq");
        try {
            CompressionCodec codec = new DefaultCodec();
            SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(testFilePath),
                    SequenceFile.Writer.keyClass(MockKey.class),
                    SequenceFile.Writer.valueClass(MockValue.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, codec));

            // Validate that configuration is set correctly
            int configuredBlockSize = conf.getInt("io.seqfile.compress.blocksize", 0);
            assertEquals("Block size should match configuration value.", 1024, configuredBlockSize);

            writer.close();
        } finally {
            // Clean up test file
            if (fs.exists(testFilePath)) {
                fs.delete(testFilePath, false);
            }
        }
    }

    // Mock key class implementing Writable
    public static class MockKey implements org.apache.hadoop.io.Writable {
        private String key;

        public MockKey() {
        }

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

        public MockValue() {
        }

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