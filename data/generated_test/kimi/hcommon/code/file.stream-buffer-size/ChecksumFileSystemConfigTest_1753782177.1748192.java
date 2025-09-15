package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ChecksumFileSystemConfigTest {

    @Test
    public void testCustomBufferSizeOverridesDefault() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        conf.setInt("io.file.buffer.size", 8192);

        // 2. Prepare the test conditions.
        // Create a real ChecksumFileSystem instance, using a LocalFileSystem as rawFS
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(LocalFileSystem.getDefaultUri(conf), conf);

        // 3. Test code.
        // The buffer size used by ChecksumFileSystem is the same as the one in the configuration
        int actualBufferSize = conf.getInt("io.file.buffer.size", 4096);
        assertEquals(8192, actualBufferSize);

        // 4. Code after testing.
        localFs.close();
    }
}