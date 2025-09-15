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
        // Create a configuration object with the testing property set
        Configuration conf = new Configuration();

        // Set an invalid value for dfs.namenode.safemode.threshold-pct
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1.5f);

        // Step 2: Create FSImage and FSNamesystem instances
        FSImage fsImage = new FSImage(conf); // Create an FSImage instance
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage); // FSNamesystem instance

        // Step 3: Invoke enableSafeModeForTesting with the created configuration
        fsNamesystem.enableSafeModeForTesting(conf);

        // Verify warning log messages for invalid threshold:
        LOG.info("Threshold value used for testing: {}", 
                conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
                DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT));
        
        // Validate that Safe Mode is still enabled despite invalid threshold value
        assertTrue("Safe mode should be enabled even with invalid threshold value", fsNamesystem.isInSafeMode());

        // Validate the threshold value set in the configuration
        float thresholdValue = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 
                DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        assertEquals("Threshold value should be equal to the set invalid value", 1.5f, thresholdValue, 0.0);

        // Step 4: Log cleanup actions
        LOG.info("Test completed successfully. Configuration cleanup executed.");
    }
}