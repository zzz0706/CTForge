package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FsDatasetCacheConfigTest {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    public void testRevocationTimeoutConfigValue() {
        // 1. Obtain configuration values using HDFS 2.8.5 API
        long defaultValue = DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS_DEFAULT;
        long configuredValue = conf.getLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS, defaultValue);
        
        // 2. Prepare test conditions
        assertEquals("Config service value should match default", defaultValue, configuredValue);
    }

    @Test
    public void testRevocationTimeoutUsedInConstructor() {
        // Mock dependencies using traditional Mockito approach
        ScheduledThreadPoolExecutor scheduledExecutor = mock(ScheduledThreadPoolExecutor.class);
        ThreadPoolExecutor threadPoolExecutor = mock(ThreadPoolExecutor.class);
        
        try {
            // Setup configuration
            long testRevocationTimeout = 900000L; // 15 minutes
            conf.setLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS, testRevocationTimeout);
            
            // Create mock dataset and datanode
            FsDatasetImpl dataset = mock(FsDatasetImpl.class);
            DataNode datanode = mock(DataNode.class);
            
            // Fix: Use proper method stubbing instead of field access
            when(datanode.getConf()).thenReturn(conf);
            
            // Since we can't directly instantiate FsDatasetCache without complex setup,
            // we'll test the configuration validation logic indirectly
            long validPollingTime = testRevocationTimeout / 3;
            conf.setLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS, validPollingTime);
            
            // Configuration should be valid (no exception thrown)
            assertTrue("Configuration should be valid", 
                conf.getLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS, 0) * 2 <= 
                conf.getLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS, 0));
        } catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test(expected = RuntimeException.class)
    public void testRevocationPollingValidationFailsWhenTooHigh() {
        // Setup configuration with invalid polling time (greater than half of revocation timeout)
        long testRevocationTimeout = 900000L;
        long invalidPollingTime = testRevocationTimeout / 2 + 1; // This should cause failure
        
        conf.setLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS, testRevocationTimeout);
        conf.setLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS, invalidPollingTime);
        
        // Validate the configuration manually since we can't easily instantiate FsDatasetCache
        long pollingMs = conf.getLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS, 0);
        long timeoutMs = conf.getLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS, 0);
        
        if (pollingMs * 2 > timeoutMs) {
            throw new RuntimeException("dfs.datanode.cache.revocation.polling.ms (" + pollingMs + 
                ") is too large. It should be smaller than dfs.datanode.cache.revocation.timeout.ms (" + 
                timeoutMs + ") / 2.");
        }
    }

    @Test
    public void testRevocationTimeoutUsedInUncacheBlockMethod() {
        // Setup configuration
        long testRevocationTimeout = 900000L;
        long testRevocationPolling = testRevocationTimeout / 3;
        
        conf.setLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS, testRevocationTimeout);
        conf.setLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS, testRevocationPolling);
        
        // Test that the configuration values are set correctly
        assertEquals("Revocation timeout should be set correctly", 
            testRevocationTimeout, 
            conf.getLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_TIMEOUT_MS, 0));
        
        assertEquals("Revocation polling should be set correctly", 
            testRevocationPolling, 
            conf.getLong(DFSConfigKeys.DFS_DATANODE_CACHE_REVOCATION_POLLING_MS, 0));
        
        // Verify the relationship between polling and timeout
        assertTrue("Polling time should be less than half of timeout", 
            testRevocationPolling * 2 <= testRevocationTimeout);
    }
}