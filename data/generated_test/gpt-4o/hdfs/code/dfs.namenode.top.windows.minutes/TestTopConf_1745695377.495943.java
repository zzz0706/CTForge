package org.apache.hadoop.hdfs.server.namenode.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import org.junit.Assert;

public class TestTopConf {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testTopConfInvalidPeriodFormat() {
        // Step 1: Create a Configuration object and set invalid values for 'dfs.namenode.top.windows.minutes'
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY, "a,b,c");

        // Step 2: Attempt to create a TopConf object and verify behavior under invalid format
        try {
            new TopConf(conf);
            Assert.fail("Expected NumberFormatException was not thrown");
        } catch (NumberFormatException e) {
            // Step 3: Verify that a NumberFormatException is thrown
            Assert.assertEquals("For input string: \"a\"", e.getMessage());
        } catch (Exception e) {
            Assert.fail("Unexpected exception type: " + e.getMessage());
        }
    }
}