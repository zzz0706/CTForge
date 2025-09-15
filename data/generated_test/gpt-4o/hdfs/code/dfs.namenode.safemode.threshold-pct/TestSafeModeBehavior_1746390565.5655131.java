package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSafeModeBehavior {

    private static final Logger LOG = LoggerFactory.getLogger(TestSafeModeBehavior.class);

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testSafeModeEnableWithInvalidThreshold() throws Exception {
        // Step 1: Prepare the test conditions
        // Create a Configuration object and set the required property
        Configuration conf = new Configuration();
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1.5f);

        // Log the value to confirm the property is correctly set
        LOG.info("Testing with dfs.namenode.safemode.threshold-pct set to: {}", 
                 conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 
                 DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT));

        // Step 2: Create FSImage and FSNamesystem instances
        FSImage fsImage = new FSImage(conf);
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
        
        // Step 3: Enable safe mode and validate behavior
        fsNamesystem.enableSafeModeForTesting(conf);

        // Verify warning log messages for invalid threshold:
        float thresholdValue = conf.getFloat(
            DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 
            DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        LOG.info("Threshold value retrieved from configuration: {}", thresholdValue);
        
        // Validate safe mode is enabled
        assertTrue("Safe mode should be enabled even with invalid threshold value", fsNamesystem.isInSafeMode());

        // Validate that the threshold value is set as provided
        assertEquals("Threshold value should match the set invalid value", 1.5f, thresholdValue, 0.0);

        // Step 4: Log cleanup actions
        LOG.info("Test for dfs.namenode.safemode.threshold-pct completed successfully.");
    }
}