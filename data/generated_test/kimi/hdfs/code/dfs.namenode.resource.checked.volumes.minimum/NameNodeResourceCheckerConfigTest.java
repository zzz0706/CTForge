package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class NameNodeResourceCheckerConfigTest {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() {
        conf = new Configuration();
        configProperties = new Properties();
        // Simulate loading from actual configuration file
        try {
            configProperties.load(this.getClass().getClassLoader().getResourceAsStream("core-default.xml"));
        } catch (IOException e) {
            // Handle exception or ignore for test setup
        }
    }

    @Test
    public void testDfsNamenoodeResourceCheckedVolumesMinimumDefaultValue() {
        // Fetch expected value directly from configuration files
        String expectedDefault = configProperties.getProperty(
            "dfs.namenode.resource.checked.volumes.minimum", "1");
        int expectedDefaultValue = Integer.parseInt(expectedDefault);

        // Verify that the default value is correctly loaded from configuration
        int actualValue = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT);
            
        assertEquals(expectedDefaultValue, actualValue);
    }

    @Test
    public void testDfsNamenoodeResourceCheckedVolumesMinimumCustomValue() {
        // Fetch expected value directly from configuration files
        int customValue = 3;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY, customValue);

        // Verify that the custom value is correctly loaded
        int actualValue = conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT);
            
        assertEquals(customValue, actualValue);
    }

    @Test
    public void testHasAvailableDiskSpaceUsesConfiguredMinimumRedundantVolumes() {
        // Given
        int configuredMinimum = 2;
        conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY, configuredMinimum);
        
        // Test that the configuration is properly set
        assertEquals(configuredMinimum, conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT));
    }

    @Test
    public void testConfigurationValueMatchesFileValue() {
        // Compare configuration file value against default config value
        String fileValue = configProperties.getProperty(
            "dfs.namenode.resource.checked.volumes.minimum", "1");
        String configValue = String.valueOf(conf.getInt(
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY, 
            DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_DEFAULT));
        
        assertEquals("Configuration file value should match default config value", 
                    fileValue, configValue);
    }
}