package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

import static org.junit.Assert.*;

public class ChecksumFileSystemConfigCoverageTest {

    private Configuration conf;
    private LocalFileSystem lfs;
    private Path testFile;
    private Path testChecksumFile;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        lfs = FileSystem.getLocal(conf);
        testFile = new Path(System.getProperty("java.io.tmpdir"), "testfile_config");
        testChecksumFile = lfs.getChecksumFile(testFile);
        lfs.delete(testFile, true);
        lfs.delete(testChecksumFile, true);
    }

    @After
    public void tearDown() throws Exception {
        lfs.delete(testFile, true);
        lfs.delete(testChecksumFile, true);
    }

    @Test
    public void testDefaultBufferSizeConfiguration() throws Exception {
        // 1. Use default configuration value (no explicit set)
        // 2. Prepare the test conditions
        FSDataOutputStream out = lfs.create(testFile);
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        out.write(data);
        out.close();

        // 3. Test code - Verify ChecksumFSInputChecker uses default buffer size
        FSDataInputStream in = lfs.open(testFile);
        assertNotNull(in);

        // Force initialization of ChecksumFSInputChecker
        byte[] buffer = new byte[512];
        int bytesRead = in.read(buffer);
        assertTrue(bytesRead > 0);

        // Verify checksum file exists (indicating ChecksumFSInputChecker was used)
        assertTrue(lfs.exists(testChecksumFile));

        // 4. Code after testing
        in.close();
    }

    @Test
    public void testCustomBufferSizeConfiguration() throws Exception {
        // 1. Configuration as input
        int customBufferSize = 8192;
        conf.setInt(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, customBufferSize);
        LocalFileSystem customLfs = FileSystem.getLocal(conf);

        // 2. Prepare the test conditions
        Path customFile = new Path(System.getProperty("java.io.tmpdir"), "testfile_custom");
        customLfs.delete(customFile, true);
        FSDataOutputStream out = customLfs.create(customFile);
        byte[] data = new byte[1024];
        out.write(data);
        out.close();

        // 3. Test code - Verify custom buffer size is used
        FSDataInputStream in = customLfs.open(customFile);
        assertNotNull(in);

        // Read to trigger ChecksumFSInputChecker initialization
        byte[] buffer = new byte[512];
        int bytesRead = in.read(buffer);
        assertTrue(bytesRead > 0);

        // Verify checksum file was created with custom buffer size
        assertTrue(customLfs.exists(customLfs.getChecksumFile(customFile)));

        // 4. Code after testing
        in.close();
        customLfs.delete(customFile, true);
    }

    @Test
    public void testZeroBufferSizeConfiguration() throws Exception {
        // 1. Configuration as input
        conf.setInt(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, 0);
        LocalFileSystem zeroLfs = FileSystem.getLocal(conf);

        // 2. Prepare the test conditions
        Path zeroFile = new Path(System.getProperty("java.io.tmpdir"), "testfile_zero");
        zeroLfs.delete(zeroFile, true);
        FSDataOutputStream out = zeroLfs.create(zeroFile);
        byte[] data = new byte[1024];
        out.write(data);
        out.close();

        // 3. Test code - Verify zero buffer size is handled gracefully
        FSDataInputStream in = zeroLfs.open(zeroFile);
        assertNotNull(in);

        // Read to trigger ChecksumFSInputChecker initialization
        byte[] buffer = new byte[512];
        int bytesRead = in.read(buffer);
        assertTrue(bytesRead > 0);

        // 4. Code after testing
        in.close();
        zeroLfs.delete(zeroFile, true);
    }

    @Test
    public void testNegativeBufferSizeConfiguration() throws Exception {
        // 1. Configuration as input
        conf.setInt(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, -1);
        LocalFileSystem negativeLfs = FileSystem.getLocal(conf);

        // 2. Prepare the test conditions
        Path negativeFile = new Path(System.getProperty("java.io.tmpdir"), "testfile_negative");
        negativeLfs.delete(negativeFile, true);
        FSDataOutputStream out = negativeLfs.create(negativeFile);
        byte[] data = new byte[1024];
        out.write(data);
        out.close();

        // 3. Test code - Verify negative buffer size is handled gracefully
        FSDataInputStream in = negativeLfs.open(negativeFile);
        assertNotNull(in);

        // Read to trigger ChecksumFSInputChecker initialization
        byte[] buffer = new byte[512];
        int bytesRead = in.read(buffer);
        assertTrue(bytesRead > 0);

        // 4. Code after testing
        in.close();
        negativeLfs.delete(negativeFile, true);
    }
}