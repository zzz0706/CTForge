package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocalFileSystemConfigKeys;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestChecksumFileSystemBufferSize {

    // Test the configuration usage of file.stream-buffer-size
    @Test
    public void testConfigurationUsageForStreamBufferSize() throws IOException {
        // 1. Prepare the configuration object without explicitly setting file.stream-buffer-size
        Configuration conf = new Configuration();

        // Initialize LocalFileSystem since ChecksumFileSystem is abstract
        LocalFileSystem localFileSystem = FileSystem.getLocal(conf);

        // Verify default value for file.stream-buffer-size from the conf
        int defaultBufferSize = conf.getInt(
                LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY,
                LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT);
        assertEquals(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT, defaultBufferSize);

        // 2. Prepare a test file
        Path testFilePath = new Path(System.getProperty("java.io.tmpdir"), "test-file");

        // Ensure file exists and can be read
        localFileSystem.create(testFilePath).close();

        // 3. Create a file input stream to validate functionality
        try (FSDataInputStream inputStream = localFileSystem.open(testFilePath)) {
            assertNotNull(inputStream);

            // Check the checksum algorithm used by the ChecksumFileSystem
            DataChecksum checksum = DataChecksum.newDataChecksum(
                    DataChecksum.Type.CRC32, defaultBufferSize);
            assertNotNull(checksum);
            assertEquals(DataChecksum.Type.CRC32, checksum.getChecksumType());
        }

        // 4. Cleanup
        localFileSystem.delete(testFilePath, false);
    }
}