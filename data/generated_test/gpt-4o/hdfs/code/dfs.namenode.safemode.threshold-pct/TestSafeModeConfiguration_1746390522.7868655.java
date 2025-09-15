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
        // 1. Prepare the test conditions.
        // Create a Configuration object and set the required property
        Configuration conf = new Configuration();
        
        // Intentionally set invalid value for safemode.threshold-pct
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1.5f);

        // 2. Test code: Create FSImage and FSNamesystem and enable safe mode
        // FSImage instance is required when initializing FSNamesystem
        FSImage fsImage = new FSImage(conf);
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);

        // Enable safe mode for testing using the configuration with invalid threshold
        fsNamesystem.enableSafeModeForTesting(conf);

        // Validate logs or warnings are displayed as expected
        // Since logging checks must be captured, ensure external tools can intercept warning logs
        LOG.info("Test completed for invalid threshold in safe mode: {}",
                conf.getFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 
                DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT));

        // Safemode should be functional despite invalid threshold value; verify behavior
        assertTrue("Safe mode should be enabled", fsNamesystem.isInSafeMode());

        // 3. Code after testing: Any cleanup or restoration can be done here
        LOG.info("Safe mode testing cleanup completed.");
    }
}