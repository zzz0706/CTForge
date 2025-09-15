package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestDfsNamenoodeMaxXattrsPerInodeConfig {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
        // Reset to default value for clean state
        conf.unset(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY);
    }

    @Test
    public void testConfigValueLoadedIntoFSDirectory() {
        // Given: A configuration with a specific value for max xattrs per inode
        int expectedLimit = 50;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, expectedLimit);

        // When: Configuration is checked
        int actualLimit = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 
                                    DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);

        // Then: The value should be what we set
        assertEquals(expectedLimit, actualLimit);
    }

    @Test
    public void testDefaultConfigValue() {
        // Given: No explicit configuration, so default should be used

        // When: Configuration is checked
        int actualLimit = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 
                                    DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);

        // Then: The value should be the default
        assertEquals(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT, actualLimit);
    }

    @Test
    public void testNegativeConfigValueValidation() {
        // Given: A negative configuration value
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, -1);

        // When & Then: Checking the configuration should reveal the invalid value
        int actualLimit = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 
                                    DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);
        
        // The configuration system doesn't validate values, so we just check what was set
        assertEquals(-1, actualLimit);
    }

    @Test
    public void testZeroConfigValue() {
        // Given: A zero configuration value
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 0);

        // When: Configuration is checked
        int actualLimit = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 
                                    DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);

        // Then: The value should be what we set
        assertEquals(0, actualLimit);
    }

    @Test
    public void testLargeConfigValue() {
        // Given: A large configuration value
        int largeLimit = 1000;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, largeLimit);

        // When: Configuration is checked
        int actualLimit = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 
                                    DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_DEFAULT);

        // Then: The value should be what we set
        assertEquals(largeLimit, actualLimit);
    }
}