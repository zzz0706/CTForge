package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class DecommissionManagerActivateTest {

    private DecommissionManager decommissionManager;
    private Configuration mockConfiguration;

    @Before
    public void setUp() {
        // Prepare test conditions
        decommissionManager = mock(DecommissionManager.class);
        mockConfiguration = mock(Configuration.class);
    }

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testActivateWithNegativeBlocksPerInterval() {
        // Prerequisites: Correctly mock Configuration to simulate invalid values.
        when(mockConfiguration.getInt(
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT))
            .thenReturn(-10);

        try {
            // Steps: Invoke activate() and expect an exception
            decommissionManager.activate(mockConfiguration);
        } catch (IllegalArgumentException exception) {
            // Expected result: Verify the exception message
            String expectedMessage = "Must set a positive value for dfs.namenode.decommission.blocks.per.interval";
            verify(mockConfiguration).getInt(
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
                DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
            assertTrue(exception.getMessage().contains(expectedMessage));
        }
    }
}