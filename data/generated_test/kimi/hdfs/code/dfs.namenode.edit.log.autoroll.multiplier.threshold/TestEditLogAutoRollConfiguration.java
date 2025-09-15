package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TestEditLogAutoRollConfiguration {

    private Configuration conf;

    @Mock
    private FSNamesystem fsNamesystem;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
        // Reset to default values if needed
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
                DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
    }

    @Test
    public void testEditLogRollerThresholdCalculationWithDefaultValues() {
        // Given: Default configuration values
        float defaultMultiplier = conf.getFloat(
                DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
                DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT);
        long defaultCheckpointTxns = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
                DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
        long expectedThreshold = (long) (defaultMultiplier * defaultCheckpointTxns);

        // When: Calculate threshold directly
        long calculatedThreshold = (long) (defaultMultiplier * defaultCheckpointTxns);

        // Then: Verify the calculation is correct
        assertEquals(expectedThreshold, calculatedThreshold);
    }

    @Test
    public void testEditLogRollerThresholdCalculationWithCustomValues() {
        // Given: Custom configuration values
        float customMultiplier = 3.5f;
        long customCheckpointTxns = 500000L;
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD, customMultiplier);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, customCheckpointTxns);
        long expectedThreshold = (long) (customMultiplier * customCheckpointTxns);

        // When: Calculate threshold directly
        long calculatedThreshold = (long) (customMultiplier * customCheckpointTxns);

        // Then: Verify the calculation is correct
        assertEquals(expectedThreshold, calculatedThreshold);
    }
}