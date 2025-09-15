package org.apache.hadoop.hdfs.server.namenode.top.window;

import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager;

import static org.junit.Assert.fail;

public class RollingWindowManagerTest {

    private Configuration conf;

    @Before
    public void setUp() {
        // Prepare the test configuration
        conf = new Configuration();
    }

    @Test
    public void testTopNInitializationWithTopUsersCntZero() {
        // Test code
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        int topUsersCnt = conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT);

        // 2. Prepare the test conditions.
        // Set DFSConfigKeys.NNTOP_NUM_USERS_KEY to 0
        conf.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, 0);

        // 3. Test code.
        try {
            // Attempt to instantiate RollingWindowManager with zero configured top users count.
            new RollingWindowManager(conf, 1000); // arbitrary reportingPeriodMs value
            fail("Expected IllegalArgumentException, but none was thrown.");
        } catch (IllegalArgumentException exception) {
            // Assert that the exception message matches the expected result
            String expectedMessage = "the number of requested top users must be at least 1";
            assert exception.getMessage().contains(expectedMessage);
        }

        // 4. Code after testing.
        // Restore the configuration to the original value (clean-up).
        conf.setInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, topUsersCnt);
    }
}