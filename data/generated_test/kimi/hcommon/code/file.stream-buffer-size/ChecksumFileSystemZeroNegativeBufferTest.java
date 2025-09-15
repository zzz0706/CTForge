package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.*;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class ChecksumFileSystemZeroNegativeBufferTest {

    @Test
    public void testZeroOrNegativeConfigValueHandledGracefully() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();
        conf.setInt("file.stream-buffer-size", 0);  // zero value

        // 2. Prepare the test conditions.
        final int bytesPerSum = 512;
        final int defaultBufferSize = conf.getInt(
                "io.file.buffer.size",
                4096);
        final int expectedSumBufferSize = Math.max(bytesPerSum,
                Math.max(0 / bytesPerSum, defaultBufferSize));

        // 3. Test code.
        // Create a real LocalFileSystem instance instead of mocking the private inner class
        LocalFileSystem lfs = FileSystem.getLocal(conf);
        Path testFile = new Path("/tmp/testfile");
        lfs.delete(testFile, true);
        lfs.create(testFile).close();

        // 4. Code after testing.
        lfs.delete(testFile, true);
    }
}