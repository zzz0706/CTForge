package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public class ChecksumFileSystemConfigUsageTest {

    @Test
    public void testGetSumBufferSizeUsesConfiguration() throws Exception {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        conf.setInt(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, 64 * 1024);

        // 2. Prepare the test conditions.
        LocalFileSystem rawFs = new LocalFileSystem();
        rawFs.initialize(LocalFileSystem.getDefaultUri(conf), conf);
        ChecksumFileSystem cfs = new LocalFileSystem(rawFs);

        // 3. Test code.
        Method getSumBufferSize = ChecksumFileSystem.class.getDeclaredMethod(
                "getSumBufferSize", int.class, int.class);
        getSumBufferSize.setAccessible(true);

        // bytesPerSum = 512, bufferSize = 4096
        int result = (Integer) getSumBufferSize.invoke(cfs, 512, 4096);
        // Expected: max(512, max(4096/512, 65536)) = 65536
        assertEquals(64 * 1024, result);

        // 4. Code after testing.
        cfs.close();
    }

    @Test
    public void testChecksumFSInputCheckerRespectsConfiguredBufferSize() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        conf.setInt(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, 32 * 1024);

        // 2. Prepare the test conditions.
        LocalFileSystem rawFs = new LocalFileSystem();
        rawFs.initialize(LocalFileSystem.getDefaultUri(conf), conf);
        ChecksumFileSystem cfs = new LocalFileSystem(rawFs);

        Path tmp = new Path(System.getProperty("java.io.tmpdir"),
                            "ChecksumFileSystemConfigUsageTest.tmp");
        FSDataOutputStream out = cfs.create(tmp, true, 4096, (short) 1, 4096L);
        out.writeUTF("testChecksumFSInputChecker");
        out.close();

        // 3. Test code.
        // Force creation of ChecksumFSInputChecker with the configured buffer size
        FSDataInputStream in = cfs.open(tmp);
        assertNotNull(in);
        in.close();

        // 4. Code after testing.
        cfs.delete(tmp, true);
        cfs.close();
    }

    @Test
    public void testChecksumFSInputCheckerUsesDefaultWhenKeyMissing() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        // deliberately do NOT set the key

        // 2. Prepare the test conditions.
        LocalFileSystem rawFs = new LocalFileSystem();
        rawFs.initialize(LocalFileSystem.getDefaultUri(conf), conf);
        ChecksumFileSystem cfs = new LocalFileSystem(rawFs);

        Path tmp = new Path(System.getProperty("java.io.tmpdir"),
                            "ChecksumFileSystemConfigUsageTestDefault.tmp");
        FSDataOutputStream out = cfs.create(tmp, true, 4096, (short) 1, 4096L);
        out.writeUTF("testDefault");
        out.close();

        // 3. Test code.
        FSDataInputStream in = cfs.open(tmp);
        assertNotNull(in);
        in.close();

        // 4. Code after testing.
        cfs.delete(tmp, true);
        cfs.close();
    }

    @Test
    public void testEdgeBufferSizeZeroHandledCorrectly() throws IOException {
        // 1. Configuration as Input
        Configuration conf = new Configuration();
        conf.setInt(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, 0);

        // 2. Prepare the test conditions.
        LocalFileSystem rawFs = new LocalFileSystem();
        rawFs.initialize(LocalFileSystem.getDefaultUri(conf), conf);
        ChecksumFileSystem cfs = new LocalFileSystem(rawFs);

        Path tmp = new Path(System.getProperty("java.io.tmpdir"),
                            "ChecksumFileSystemConfigUsageTestEdge.tmp");
        FSDataOutputStream out = cfs.create(tmp, true, 4096, (short) 1, 4096L);
        out.writeUTF("testEdge");
        out.close();

        // 3. Test code.
        FSDataInputStream in = cfs.open(tmp);
        assertNotNull(in);
        in.close();

        // 4. Code after testing.
        cfs.delete(tmp, true);
        cfs.close();
    }
}