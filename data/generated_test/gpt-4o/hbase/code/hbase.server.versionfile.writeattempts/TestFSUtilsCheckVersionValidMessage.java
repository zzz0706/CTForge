package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.exceptions.DeserializationException;

@Category(SmallTests.class)
public class TestFSUtilsCheckVersionValidMessage {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestFSUtilsCheckVersionValidMessage.class);

    private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    @Test
    public void test_fs_utils_check_version_valid_message() throws IOException, DeserializationException {
        // Prepare the test conditions
        Configuration configuration = TEST_UTIL.getConfiguration();
        FileSystem fs = FileSystem.get(configuration);
        Path rootDir = TEST_UTIL.getDataTestDir("hbase-root");

        // Use the API correctly to obtain configuration values instead of hardcoding them
        int threadWakeFrequency = configuration.getInt("hbase.server.thread.wakefrequency", 10 * 1000);
        int versionFileWriteAttempts = configuration.getInt("hbase.server.versionfile.writeattempts", 3);

        FSUtils.setVersion(fs, rootDir, threadWakeFrequency, versionFileWriteAttempts);

        // Test code
        FSUtils.checkVersion(fs, rootDir, true);

        // Code after testing
        TEST_UTIL.cleanupTestDir();
    }
}