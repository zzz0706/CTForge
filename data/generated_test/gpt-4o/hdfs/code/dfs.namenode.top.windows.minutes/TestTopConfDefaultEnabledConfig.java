package org.apache.hadoop.hdfs.server.namenode.top;

import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import static org.junit.Assert.assertEquals;

public class TestTopConfDefaultEnabledConfig {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testTopConfDefaultEnabledConfig() {
        // Step 1: Create a Configuration object without setting 'dfs.namenode.top.enabled'.
        Configuration conf = new Configuration();

        // Step 2: Pass the Configuration object to the TopConf constructor.
        TopConf topConf = new TopConf(conf);

        // Step 3: Verify that the isEnabled field matches DFSConfigKeys.NNTOP_ENABLED_DEFAULT.
        assertEquals(DFSConfigKeys.NNTOP_ENABLED_DEFAULT, topConf.isEnabled);
    }
}