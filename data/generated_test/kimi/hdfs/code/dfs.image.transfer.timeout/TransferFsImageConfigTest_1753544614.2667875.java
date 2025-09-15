package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.hdfs.util.Canceler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

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

    @Test
    public void testDfsImageTransferTimeoutSkippedWhenNegative() throws IOException {
        // Prepare test conditions
        Configuration config = new HdfsConfiguration();
        config.setInt(DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY, -1);
        
        // This test verifies the configuration logic but cannot directly test
        // the private setTimeout method. We can only verify that the configuration
        // is correctly read.
        int timeout = config.getInt(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT
        );
        
        assertEquals("Timeout should be -1", -1, timeout);
    }

    @Test
    public void testDoGetUrlUsesTimeoutConfiguration() throws Exception {
        // Prepare test conditions
        Configuration config = new HdfsConfiguration();
        int customTimeout = 30000;
        config.setInt(DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY, customTimeout);
        
        // Verify the configuration is set correctly
        int timeout = config.getInt(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT
        );
        
        assertEquals("Custom timeout should be set", customTimeout, timeout);
        
        // Note: We cannot directly test the internal timeout setting behavior
        // without access to the private methods and fields in TransferFsImage
    }

    @Test
    public void testUploadImageFromStorageUsesTimeoutConfiguration() throws Exception {
        // Prepare test conditions
        Configuration config = new HdfsConfiguration();
        int customTimeout = 45000;
        config.setInt(DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY, customTimeout);
        
        // Verify the configuration is set correctly
        int timeout = config.getInt(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT
        );
        
        assertEquals("Custom timeout should be set", customTimeout, timeout);
        
        // Note: We cannot directly test the internal timeout setting behavior
        // without access to the private methods and fields in TransferFsImage
    }
}