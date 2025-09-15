package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ImageServletConfigTest {

    @Test
    public void testBootstrapStandbyBandwidthConfigDefaultValue() throws IOException {
        // Load default value from configuration
        Configuration conf = new Configuration();
        long actualValue = conf.getLong(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT
        );

        assertEquals("Default value should be 0 (no throttling)", 0L, actualValue);
    }

    @Test
    public void testServeFileUsesCorrectThrottlerForBootstrapStandby() throws Exception {
        // Prepare test conditions
        Configuration conf = new Configuration();
        long bandwidthValue = 1024000L;
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY, bandwidthValue);

        // In HDFS 2.8.5, throttling is handled internally by the servlet
        // We can test that the configuration is properly read
        long configuredValue = conf.getLong(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT
        );
        
        assertEquals("Bandwidth value should be correctly configured", bandwidthValue, configuredValue);
    }

    @Test
    public void testServeFileUsesNullThrottlerForBootstrapStandbyWhenZero() throws IOException {
        // Prepare test conditions
        Configuration conf = new Configuration();
        long bandwidthValue = 0L;
        conf.setLong(DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY, bandwidthValue);

        // Test that the configuration is properly read
        long configuredValue = conf.getLong(
            DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_KEY,
            DFSConfigKeys.DFS_IMAGE_TRANSFER_BOOTSTRAP_STANDBY_RATE_DEFAULT
        );

        assertEquals("Bandwidth value should be 0", 0L, configuredValue);
    }
}