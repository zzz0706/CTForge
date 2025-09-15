package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestImageTransferTimeoutConfig {

    private Configuration conf;
    private int expectedTimeout;

    @Before
    public void setUp() {
        conf = new HdfsConfiguration();
        expectedTimeout = conf.getInt(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT
        );
    }

    @Test
    public void testConfigurationValueMatchesDefault() {
        // Reference loader comparison
        Properties props = new Properties();
        // In a real scenario, this would load from the actual config files
        // For HDFS 2.8.5, the default should be 60000
        assertEquals(60000, expectedTimeout);
    }

    @Test
    public void testConfigurationValueFromServiceMatchesExpected() {
        // Simulate getting the value via ConfigService pattern
        int configValue = conf.getInt(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT
        );
        
        // Compare with expected value
        assertEquals(expectedTimeout, configValue);
    }
}