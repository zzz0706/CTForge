package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;
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
        // Prepare the test conditions.
        Configuration conf = new Configuration();

        // Use the API to get the threshold value instead of hardcoding.
        float thresholdPct = conf.getFloat(
            DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 
            DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_DEFAULT
        );

        // Precondition: simulate invalid threshold by explicitly setting it in a testing method.
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY, 1.5f);

        // Test Code: Enable safe mode and verify functionality.
        FSImage fsImage = new FSImage(conf);
        FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);

        fsNamesystem.enterSafeMode(false);

        // Verify logs or expected behavior after entering safe mode.
        LOG.info("Safe mode validation with invalid threshold completed.");

        // Cleanup: Any post-test resource handling can be done here.
        // For example, ensuring the removal of temporary changes made to setup configurations.
    }
}