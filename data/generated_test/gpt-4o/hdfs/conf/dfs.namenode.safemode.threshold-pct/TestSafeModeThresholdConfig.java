package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;

public class TestSafeModeThresholdConfig {

    @Test
    public void testSafeModeThresholdPctConfiguration() {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // The correct keys are defined in DFSConfigKeys, which holds the configuration constants for HDFS.

        // 2. Prepare the test conditions: Load the configuration using the HDFS API
        Configuration conf = new Configuration();

        // 3. Test code:
        // Retrieve the configuration value for dfs.namenode.safemode.threshold-pct using DFSConfigKeys
        float threshold = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        
        // Validate constraints: Ensure the value falls within the correct range
        // Values <= 0 mean no waiting for a particular percentage, values > 1 make safe mode permanent
        try {
            // Check if the value is valid (within the range)
            if (threshold < 0.0f) {
                Assert.fail("Invalid configuration: dfs.namenode.safemode.threshold-pct cannot be less than 0. Value: " + threshold);
            }
            if (threshold > 1.0f) {
                Assert.fail("Invalid configuration: dfs.namenode.safemode.threshold-pct must not exceed 1. Value: " + threshold);
            }
        } catch (Exception e) {
            Assert.fail("An error occurred while validating the configuration: " + e.getMessage());
        }

        // 4. Code after testing: Clean up or log success for debugging
        System.out.println("Configuration valid: dfs.namenode.safemode.threshold-pct = " + threshold);
    }
}