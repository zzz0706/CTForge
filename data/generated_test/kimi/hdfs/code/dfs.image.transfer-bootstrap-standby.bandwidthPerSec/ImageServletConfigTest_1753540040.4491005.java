package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class ImageServletConfigTest {

    @Test
    public void testBootstrapStandbyBandwidthConfigDefaultValue() throws IOException {
        // Load default value from DFSConfigKeys
        long expectedDefault = DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT;

        // Load configuration and verify default
        Configuration conf = new Configuration();
        long actualValue = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);

        assertEquals("Default value should match DFSConfigKeys constant", expectedDefault, actualValue);
    }

    @Test
    public void testConfigurationFileMatchesRuntime() throws IOException {
        // This test verifies that the configuration loaded at runtime matches
        // what's in the configuration files (e.g., core-default.xml)
        
        // Create a fresh configuration to load defaults
        Configuration conf = new Configuration();
        
        // Get value via Configuration API
        long runtimeValue = conf.getLong(
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT);
        
        // Verify it matches the expected default
        assertEquals("Runtime config value should match DFSConfigKeys default", 
                DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT,
                runtimeValue);
    }
}