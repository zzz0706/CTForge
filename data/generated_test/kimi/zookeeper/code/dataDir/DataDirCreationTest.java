package org.apache.zookeeper.server;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataDirCreationTest {

    private static final String TEST_DATA_DIR = "/tmp/zktest/data";

    @Before
    public void setUp() {
        File dataDir = new File(TEST_DATA_DIR);
        if (dataDir.exists()) {
            deleteRecursively(dataDir);
        }
    }

    @After
    public void tearDown() {
        File dataDir = new File(TEST_DATA_DIR);
        if (dataDir.exists()) {
            deleteRecursively(dataDir);
        }
    }

    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    @Test
    public void dataDirIsCreatedIfMissing() throws IOException {
        // 1. You need to use the zookeeper 2.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        String expectedDataDir = TEST_DATA_DIR;

        // 2. Prepare the test conditions.
        File dataDir = new File(expectedDataDir);
        assertFalse("Directory must not exist before test", dataDir.exists());

        // 3. Test code.
        // FileTxnSnapLog will create the directory if it does not exist
        FileTxnSnapLog txnLog = new FileTxnSnapLog(new File(expectedDataDir), new File(expectedDataDir));

        // 4. Code after testing.
        assertTrue("Directory should be created: " + expectedDataDir, dataDir.exists());
        assertTrue("Directory should be a directory: " + expectedDataDir, dataDir.isDirectory());
    }
}