package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TransferFsImageConfigTest {

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
        // Compare with ConfigService/HdfsConfiguration
        Configuration config = new HdfsConfiguration();
        int configValue = config.getInt(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT
        );

        assertEquals(
            "Configuration value should match default",
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT,
            configValue
        );
    }

    @Test
    public void testGetImageTransferTimeoutFromConfiguration() {
        // Test that we can retrieve the timeout configuration correctly
        Configuration config = new HdfsConfiguration();
        int timeout = config.getInt(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT
        );
        
        // Verify it matches the expected default value
        assertEquals(
            "Image transfer timeout should match default value",
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT,
            timeout
        );
    }

    @Test
    public void testCustomImageTransferTimeout() {
        // Test with a custom timeout value
        Configuration config = new HdfsConfiguration();
        int customTimeout = 60000; // 60 seconds
        config.setInt(DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY, customTimeout);
        
        int timeout = config.getInt(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT
        );
        
        assertEquals(
            "Custom image transfer timeout should be used",
            customTimeout,
            timeout
        );
    }
}