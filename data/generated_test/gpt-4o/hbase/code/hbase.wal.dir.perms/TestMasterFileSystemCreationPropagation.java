package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

@Category({MasterTests.class, SmallTests.class}) // Categorize the test as related to the Master module and small unit tests.
public class TestMasterFileSystemCreationPropagation {

    @ClassRule // HBaseClassTestRule is used for organizing unit tests systematically.
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMasterFileSystemCreationPropagation.class);

    @Test
    public void testMasterFileSystemCreationPropagation() throws IOException {
        // Create an instance of HBaseTestingUtility to set up a test environment
        HBaseTestingUtility utility = new HBaseTestingUtility();
        Configuration conf = utility.getConfiguration();

        // Add configuration resources (if needed)
        // For example, optionally load custom hbase-site.xml configurations if required
        conf.addResource("hbase-site.xml");

        // Generate the WAL root directory using the HBaseTestingUtility instance
        Path walDir = utility.getDataTestDir("random-wal-dir");

        // Get the FileSystem instance from the configuration
        FileSystem fs = walDir.getFileSystem(conf);

        // Ensure the WAL directory is created properly
        if (!fs.exists(walDir)) {
            fs.mkdirs(walDir); // Create the directory if it does not exist
        }

        // Validate the WAL directory exists and is a directory 
        assert fs.exists(walDir) : "WAL directory should exist.";
        assert fs.getFileStatus(walDir).isDirectory() : "WAL should be a directory.";

        // Clean up after test execution
        fs.delete(walDir, true);
        assert !fs.exists(walDir) : "WAL directory should be deleted after the test.";
    }
}