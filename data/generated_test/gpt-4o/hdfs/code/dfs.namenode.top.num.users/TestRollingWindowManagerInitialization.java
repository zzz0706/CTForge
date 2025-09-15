package org.apache.hadoop.hdfs.server.namenode.top.window;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestRollingWindowManagerInitialization {

    @Test 
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    public void testRollingWindowManagerInitializationWithValidConfiguration() {
        // Prepare the test conditions.
        Configuration conf = new Configuration();
        
        // Use API to get the value of dfs.namenode.top.num.users and ensure it has valid output.
        int topUsersCnt = conf.getInt(DFSConfigKeys.NNTOP_NUM_USERS_KEY, DFSConfigKeys.NNTOP_NUM_USERS_DEFAULT);
        assertTrue("Configuration value for dfs.namenode.top.num.users must be >= 1", topUsersCnt >= 1);
        
        // Test code.
        int reportingPeriodMs = 60000; // Example reporting period of 60 seconds.
        RollingWindowManager rollingWindowManager = null;
        
        try {
            rollingWindowManager = new RollingWindowManager(conf, reportingPeriodMs);
        } catch (Exception e) {
            fail("RollingWindowManager initialization failed with valid configuration due to: " + e.getMessage());
        }

        assertNotNull("RollingWindowManager instance should not be null after initialization", rollingWindowManager);

        // Verify the expected constraints during initialization using reflection since topUsersCnt has private access.
        int actualTopUsersCnt = -1;
        try {
            java.lang.reflect.Field topUsersCntField = RollingWindowManager.class.getDeclaredField("topUsersCnt");
            topUsersCntField.setAccessible(true);
            actualTopUsersCnt = topUsersCntField.getInt(rollingWindowManager);
        } catch (Exception e) {
            fail("Reflection failed to access topUsersCnt field: " + e.getMessage());
        }

        assertEquals("RollingWindowManager topUsersCnt value should match the configuration", topUsersCnt, actualTopUsersCnt);
        
        // Code after testing.
        // Add teardown or cleanup operations if necessary.
    }
}