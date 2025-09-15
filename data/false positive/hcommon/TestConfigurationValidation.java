package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestConfigurationValidation {

    @Test
    public void testFileStreamBufferSizeConstraints() {
        // Step 1: Initialize Hadoop Configuration
        Configuration conf = new Configuration();

        // Step 2: Read 'file.stream-buffer-size' from configuration
        int streamBufferSize = conf.getInt(
                LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, 
                LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT); // defaults to 4096

        // Step 3: Read 'file.bytes-per-checksum' from configuration
        String bytesPerChecksumKey = "file.bytes-per-checksum"; // The key for the configuration
        int bytesPerChecksum = conf.getInt(bytesPerChecksumKey, -1); // -1 to indicate unset case for testing

        // Step 4: Validate only if both are set
        assertTrue("Invalid configuration: 'file.stream-buffer-size' should be a positive integer", 
            streamBufferSize > 0);

        assertEquals(4096, streamBufferSize);
    }
}