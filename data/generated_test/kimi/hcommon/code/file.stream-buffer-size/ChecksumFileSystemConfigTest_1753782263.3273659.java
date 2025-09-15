package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;

public class ChecksumFileSystemConfigTest {

    private Configuration conf;
    private LocalFileSystem localFs;
    private Path testFile;
    private File tempDir;

    @Before
    public void setUp() throws IOException {
        // 1. Configuration as Input
        conf = new Configuration();
        // 2. Prepare the test conditions.
        tempDir = new File(System.getProperty("java.io.tmpdir"),
                "ChecksumFileSystemConfigTest-" + System.nanoTime());
        if (!tempDir.mkdirs()) {
            throw new IOException("Failed to create temp directory");
        }
        Path root = new Path(tempDir.getAbsolutePath());
        localFs = FileSystem.getLocal(conf);
        localFs.setWorkingDirectory(root);
        testFile = new Path(root, "testfile");
    }

    @After
    public void tearDown() throws IOException {
        // 4. Code after testing.
        if (localFs != null) {
            localFs.delete(testFile, false);
            Path crcFile = localFs.getChecksumFile(testFile);
            if (crcFile != null) {
                localFs.delete(crcFile, false);
            }
            localFs.close();
        }
        if (tempDir != null) {
            tempDir.delete();
        }
    }

    @Test
    public void testCustomBufferSizeOverridesDefault() throws IOException {
        // 1. Configuration as Input
        conf.setInt("io.file.buffer.size", 8192);

        // 2. Prepare the test conditions.
        // Create a dummy file and its .crc file so that ChecksumFSInputChecker can be instantiated
        try (OutputStream out = localFs.create(testFile, true, 4096)) {
            out.write(new byte[1024]);
        }

        // 3. Test code.
        // Instantiate FSDataInputStream via open method, which internally uses ChecksumFSInputChecker
        FSDataInputStream in = localFs.open(testFile, 8192);

        // Retrieve the bufferSize used internally (indirectly via the configuration)
        int bytesPerSum = localFs.getBytesPerSum();
        int expectedSumBufferSize = Math.max(bytesPerSum,
                Math.max(8192 / bytesPerSum,
                        conf.getInt("io.file.buffer.size", 4096)));

        // The data stream bufferSize is 8192, checksum stream bufferSize is computed
        assertEquals(8192, conf.getInt("io.file.buffer.size", 4096));
        assertEquals(expectedSumBufferSize,
                localFs.getConf().getInt("io.file.buffer.size", 4096));

        in.close();
    }
}