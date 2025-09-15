package org.apache.hadoop.io; 

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.io.SequenceFile;  
import org.apache.hadoop.io.compress.CompressionCodec;  
import org.apache.hadoop.io.compress.DefaultCodec;  
import org.junit.Assert;  
import org.junit.Test;  

public class SequenceFileTest {  

    @Test
    public void testConfigurationDefaultFallback() {  
        // Prepare testing conditions by initializing the configuration.
        Configuration conf = new Configuration();
        conf.setInt("io.seqfile.compress.blocksize", 100000); // Set test-specific configurations.

        try {
            // Instantiate a SequenceFile.Writer using its builder.
            CompressionCodec codec = new DefaultCodec();
            SequenceFile.Writer writer = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(new org.apache.hadoop.fs.Path("test.seq")),
                SequenceFile.Writer.keyClass(org.apache.hadoop.io.Text.class),
                SequenceFile.Writer.valueClass(org.apache.hadoop.io.Text.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, codec)
            );

            // Access the configuration within the writer directly using the API.
            int compressionBlockSize = conf.getInt("io.seqfile.compress.blocksize", -1); 

            // Validate that the fallback value was applied correctly.
            Assert.assertTrue("The configuration value for compressionBlockSize does not match expectations.",
                    compressionBlockSize > 0);

            // Close the writer after the test.
            writer.close();

        } catch (Exception e) {
            // Fail the test with a proper message if an exception occurs.
            Assert.fail("Unexpected exception: " + e.getMessage());
        }
    }  
}