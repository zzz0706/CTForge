package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class TestXFrameOptionsConfiguration {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration(false);
        configProperties = new Properties();
        // Load default configuration values
        configProperties.setProperty("dfs.xframe.enabled", "true");
        configProperties.setProperty("dfs.xframe.value", "SAMEORIGIN");
    }

    @Test
    public void testDfsXFrameValue_DefaultValue() throws Exception {
        // Prepare test conditions
        String expectedValue = configProperties.getProperty("dfs.xframe.value");
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, "true");

        // Test code - simulate HTTP server initialization logic
        final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

        final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

        // Verify the configuration value is correctly retrieved
        assertEquals(Boolean.parseBoolean(configProperties.getProperty("dfs.xframe.enabled")), xFrameEnabled);
        assertEquals(expectedValue, xFrameOptionValue);
    }

    @Test
    public void testDfsXFrameValue_CustomValue_DENY() throws Exception {
        // Prepare test conditions
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, "DENY");
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, "true");
        String expectedValue = "DENY";

        // Test code - simulate NameNodeHttpServer initialization logic
        final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

        final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

        // Verify the configuration value is correctly retrieved
        assertEquals(true, xFrameEnabled);
        assertEquals(expectedValue, xFrameOptionValue);
    }

    @Test
    public void testDfsXFrameValue_CustomValue_ALLOW_FROM() throws Exception {
        // Prepare test conditions
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, "ALLOW-FROM");
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED, "true");
        String expectedValue = "ALLOW-FROM";

        // Test code
        final boolean xFrameEnabled = conf.getBoolean(
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
            DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

        final String xFrameOptionValue = conf.getTrimmed(
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE,
            DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

        // Verify the configuration value is correctly retrieved
        assertEquals(true, xFrameEnabled);
        assertEquals(expectedValue, xFrameOptionValue);
    }

    @Test
    public void testDfsXFrameValue_PropertyFileComparison() throws Exception {
        // Prepare test conditions
        conf.set(DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, "SAMEORIGIN");
        String configKey = DFSConfigKeys.DFS_XFRAME_OPTION_VALUE;
        
        // Get value via Configuration API
        String configValue = conf.getTrimmed(configKey, DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);
        
        // Get value from properties file (simulated)
        String propertyValue = configProperties.getProperty("dfs.xframe.value", DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);
        
        // Compare values
        assertEquals(propertyValue, configValue);
    }
}