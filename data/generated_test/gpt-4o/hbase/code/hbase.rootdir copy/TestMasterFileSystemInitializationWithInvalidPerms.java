package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class to validate the initialization of MasterFileSystem with invalid permissions.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestMasterFileSystemInitializationWithInvalidPerms {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestMasterFileSystemInitializationWithInvalidPerms.class);

    @Test
    public void testMasterFileSystemInitializationWithInvalidPerms() throws Exception {
        // 1. Initialize HBaseCommonTestingUtility to set up the testing environment.
        HBaseCommonTestingUtility testingUtility = new HBaseCommonTestingUtility();
        Configuration configuration = testingUtility.getConfiguration(); // Retrieve configuration using HBase 2.2.2 API.

        // 2. Prepare invalid configuration by setting an invalid `hbase.rootdir.perms` value.
        configuration.set("hbase.rootdir.perms", "invalid_perms"); // Simulating invalid value for permissions.

        // 3. Set up the root directory for HBase using the testing utility.
        Path hbaseRootDir = testingUtility.getDataTestDir("hbase-rootdir");
        FSUtils.setRootDir(configuration, hbaseRootDir);

        // 4. Attempt to initialize the MasterFileSystem and validate expected behavior.
        try {
            new MasterFileSystem(configuration);
            // If no exception is thrown, fail the test because invalid permissions should trigger an error.
            throw new AssertionError("Expected IllegalArgumentException or IOException due to invalid permissions");
        } catch (IllegalArgumentException | org.apache.hadoop.fs.permission.AccessControlException e) {
            // Catch and log the expected exception to verify correct handling of invalid permissions.
            System.out.println("Caught expected exception: " + e.getMessage());
        }

        // 5. Perform cleanup of resources after test execution.
        testingUtility.cleanupTestDir();
    }
}