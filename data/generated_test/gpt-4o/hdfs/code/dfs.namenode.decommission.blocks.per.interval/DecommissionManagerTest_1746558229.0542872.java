package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DecommissionManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HeartbeatManager;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class DecommissionManagerTest {

    @Mock
    private Configuration mockConfiguration;

    @Mock
    private Namesystem mockNamesystem;

    @Mock
    private BlockManager mockBlockManager;

    @Mock
    private HeartbeatManager mockHeartbeatManager;

    private DecommissionManager decommissionManager;

    @Before
    public void setUp() {
        // Initialize mock objects using Mockito
        MockitoAnnotations.initMocks(this);

        // Initialize DecommissionManager with mocked dependencies
        decommissionManager = new DecommissionManager(mockNamesystem, mockBlockManager, mockHeartbeatManager);
    }

    @Test
    public void testActivateWithNegativeMaxConcurrentTrackedNodes() {
        // Test prerequisites and setup:
        // 1. Configure mockConfiguration to return a negative value for 'dfs.namenode.decommission.blocks.per.interval'.
        when(mockConfiguration.getInt(
                eq("dfs.namenode.decommission.blocks.per.interval"),
                anyInt())
        ).thenReturn(-1);

        // Test execution and validation:
        // 2. Attempt to activate DecommissionManager with the mocked configuration.
        try {
            decommissionManager.activate(mockConfiguration);

            // 3. If no exception is thrown, the test fails.
            fail("Expected IllegalArgumentException to be thrown.");
        } catch (IllegalArgumentException e) {
            // 4. Verify that the exception message matches the expected one.
            assertEquals(
                "Must set a positive value for dfs.namenode.decommission.blocks.per.interval",
                e.getMessage()
            );
        }
    }
}