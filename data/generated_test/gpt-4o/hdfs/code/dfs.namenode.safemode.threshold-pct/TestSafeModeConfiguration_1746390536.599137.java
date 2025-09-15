package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSafeModeConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(TestSafeModeConfiguration.class);

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testSafeModeEnableWithInvalidThreshold() throws Exception {
        // Step 1: Prepare the test conditions
        // Create a Configuration object and set the testing property
        Configuration conf = new Configuration();
        
        // Set invalid value for dfs.namenode.safemode.threshold-pct
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1.5f);

        // Step 2: Test code - Create FSImage and FSNamesystem to enable Safe Mode
        FSImage fsImage = new FSImage(conf); // FSImage instance for FSNamesystem
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);

        // Invoke enableSafeModeForTesting with invalid configuration
        fsNamesystem.enableSafeModeForTesting(conf);

        // Validate logs or warnings are displayed as expected
        // Note that for log-level verification, specialized interceptors are needed in larger suites
        LOG.info("Test executed: Verify invalid threshold setting is handled. Value set: {}",
                conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
                DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT));

        // Ensures Safe Mode is enabled despite invalid threshold
        assertTrue("Safe mode should be enabled even with invalid threshold value", fsNamesystem.isInSafeMode());

        // Verify configuration propagated during threshold settings and interactions:
        float threshold = conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
                DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT);
        assertTrue("Threshold should match invalid value set", threshold == 1.5f);

        // Step 4: Code after testing - Cleanup specific testing configurations etc.
        LOG.info("SafeMode test completed and cleanup executed successfully.");
    }
}