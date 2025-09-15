package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestNameNodeResourceMonitor {
    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_NameNodeResourceMonitor_withSufficientResources() throws Exception {
        // Create a configuration object and set dfs.namenode.resource.check.interval
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, 5000L);
        long resourceCheckInterval = conf.getLong(DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY, DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);
        assertEquals(5000L, resourceCheckInterval);

        // Prepare the input conditions for unit testing
        FSNamesystem mockFSNamesystem = mock(FSNamesystem.class);
        FSNamesystem.NameNodeResourceMonitor monitor = mockFSNamesystem.new NameNodeResourceMonitor();

        // Mock the behavior of FSNamesystem
        when(mockFSNamesystem.nameNodeHasResourcesAvailable()).thenReturn(true);
        when(mockFSNamesystem.isRunning()).thenReturn(true);
        when(mockFSNamesystem.isInSafeMode()).thenReturn(false);
        doNothing().when(mockFSNamesystem).checkAvailableResources();

        // Test code
        monitor.run();

        // Validate the expected result: no safe mode entered
        verify(mockFSNamesystem, never()).enterSafeMode(anyBoolean());
    }
}