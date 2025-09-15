package org.apache.hadoop.hbase.wal;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

@Category(SmallTests.class)
public class TestAbstractFSWALProvider {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestAbstractFSWALProvider.class);

    @Test
    public void testGetServerNameFromWALDirectoryName_missingConfigurationKey() {
        // Test code for HBase 2.2.2
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // Create an empty configuration to simulate a missing 'hbase.rootdir' key.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        // Prepare a valid WAL directory path for testing.
        Path walPath = new Path("/hbase/.logs/serverName/logfile");

        // 3. Test code.
        try {
            // Attempt to retrieve a ServerName using the incomplete configuration and valid WAL path.
            ServerName serverName = AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, walPath.toString());
        } catch (IllegalArgumentException e) {
            // Exception is expected because 'hbase.rootdir' is not set in the configuration.
            // Verify the exception message contains the expected text.
            assert e.getMessage().contains("hbase.rootdir key not found in conf.");
        } catch (IOException e) {
            // Handle IOException as required by the method signature.
            e.printStackTrace();
            assert false : "IOException should not occur during this test.";
        }

        // 4. Code after testing.
        // No additional cleanup is required for this test case.
    }
}