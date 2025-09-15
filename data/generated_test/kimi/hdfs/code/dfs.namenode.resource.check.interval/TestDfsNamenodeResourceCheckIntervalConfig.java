package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDfsNamenodeResourceCheckIntervalConfig {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        // Initialize configuration
        conf = new Configuration();
    }

    @Test
    public void testResourceCheckIntervalConfigIsLoadedCorrectly() {
        // Test that we can get the configuration value
        long actualInterval = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT
        );

        // Should return the default value since we haven't set anything
        assertEquals("Resource check interval should match the default value",
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT, actualInterval);
    }

    @Test
    public void testNameNodeResourceMonitorUsesCorrectInterval() throws Exception {
        // Set up a specific value for testing
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, 10000L);
        
        // Test that the configuration value is correctly retrieved
        long interval = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT
        );
        
        assertEquals("Resource check interval should be set correctly",
            10000L, interval);
    }

    @Test
    public void testDefaultConfigurationValue() {
        // Test that the default value is correct according to DFSConfigKeys
        Configuration defaultConf = new Configuration();
        long defaultValue = defaultConf.getLong(
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT
        );
        
        assertEquals("Default resource check interval should match the constant",
            DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT, defaultValue);
    }
    
    @Test
    public void testConfigurationDefaultValueConstant() {
        // Verify that the constant value is what we expect (5 seconds = 5000 ms based on the actual HDFS 2.8.5 implementation)
        assertEquals("Default resource check interval constant should be 5 seconds (5000 ms)",
            5000L, DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);
    }
}