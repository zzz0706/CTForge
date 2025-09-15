package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

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
                DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_DEFAULT);
    }

    @Test
    public void testConfigurationValueMatchesDefault() {
        // Reference loader comparison
        Properties defaultProps = new Properties();
        // In a real scenario, load from hdfs-default.xml or similar source
        // Here we simulate by putting the known default
        defaultProps.setProperty(DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY, "60000");

        String defaultValue = defaultProps.getProperty(DFSConfigKeys.DFS_IMAGE_TRANSFER_TIMEOUT_KEY);
        int defaultTimeout = Integer.parseInt(defaultValue);

        assertEquals("Configuration default timeout should match expected value", 
                    defaultTimeout, expectedTimeout);
    }
}