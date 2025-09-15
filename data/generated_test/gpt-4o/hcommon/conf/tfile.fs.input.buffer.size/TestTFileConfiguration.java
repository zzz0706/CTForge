package org.apache.hadoop.io.file.tfile;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTFileConfiguration {

    /**
     * Test to validate the configuration value for "tfile.fs.input.buffer.size".
     * This configuration defines the buffer size for FSDataInputStream in bytes.
     */
    @Test
    public void testFSInputBufferSizeConfig() {
        // Step 1: Load the configuration
        Configuration conf = new Configuration();
        
        // Step 2: Retrieve the configuration value
        int fsInputBufferSize = TFile.getFSInputBufferSize(conf);
        
        // Step 3: Validate the configuration value
        // Constraints:
        // 1. Must be a positive integer.
        // 2. Default value is 262144 (256 * 1024) if not set.
        
        // Assert that the buffer size is positive
        assertTrue("Buffer size must be a positive integer", fsInputBufferSize > 0);
        
        // Assert default value
        int defaultBufferSize = 256 * 1024; // Default is 262144 bytes
        assertEquals("Default buffer size should be 262144 bytes", defaultBufferSize, fsInputBufferSize);
        
        // Additional checks if necessary
        // Ensure the buffer size is within a reasonable range (e.g., less than 1GB)
        int maxBufferSize = 1024 * 1024 * 1024; // Example constraint: maximum is 1GB
        assertTrue("Buffer size must be less than or equal to 1GB", fsInputBufferSize <= maxBufferSize);
    }
}