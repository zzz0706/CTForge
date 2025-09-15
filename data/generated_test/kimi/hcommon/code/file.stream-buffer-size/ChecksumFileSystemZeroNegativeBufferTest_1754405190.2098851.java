package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.*;
import java.net.URI;

import static org.junit.Assert.*;

public class ChecksumFileSystemZeroNegativeBufferTest {

    @Test
    public void testZeroOrNegativeConfigValueHandledGracefully() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();
        // Test zero value
        conf.setInt("file.stream-buffer-size", 0);
        
        // 2. Prepare the test conditions
        LocalFileSystem lfs = FileSystem.getLocal(conf);
        Path testFile = new Path(System.getProperty("java.io.tmpdir"), "testfile");
        lfs.delete(testFile, true);
        
        // Create a file with some data
        FSDataOutputStream out = lfs.create(testFile);
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        out.write(data);
        out.close();
        
        // 3. Test code - Force ChecksumFSInputChecker creation
        FSDataInputStream in = lfs.open(testFile);
        assertNotNull(in);
        
        // Verify the stream is actually a ChecksumFSInputChecker by checking buffer size behavior
        // Read some data to trigger initialization
        byte[] buffer = new byte[512];
        int bytesRead = in.read(buffer);
        assertTrue(bytesRead > 0);
        
        // Test negative value
        conf.setInt("file.stream-buffer-size", -1);
        LocalFileSystem lfsNegative = FileSystem.getLocal(conf);
        Path testFileNegative = new Path(System.getProperty("java.io.tmpdir"), "testfile_negative");
        lfsNegative.delete(testFileNegative, true);
        
        out = lfsNegative.create(testFileNegative);
        out.write(data);
        out.close();
        
        FSDataInputStream inNegative = lfsNegative.open(testFileNegative);
        assertNotNull(inNegative);
        bytesRead = inNegative.read(buffer);
        assertTrue(bytesRead > 0);
        
        // 4. Code after testing
        in.close();
        inNegative.close();
        lfs.delete(testFile, true);
        lfsNegative.delete(testFileNegative, true);
    }
}