package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;

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
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testEditLogAutoRollConfigurationValues() throws Exception {
        // Given: Custom configuration values
        float customMultiplier = 2.5f;
        long customCheckpointTxns = 400000L;
        conf.setFloat(DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD, customMultiplier);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, customCheckpointTxns);
        long expectedThreshold = (long) (customMultiplier * customCheckpointTxns);

        // When: Get configuration values
        float actualMultiplier = conf.getFloat(
            DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
            DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT);
        long actualCheckpointTxns = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
        long calculatedThreshold = (long) (actualMultiplier * actualCheckpointTxns);

        // Then: Verify that configuration values are set correctly
        assertEquals("Edit log auto-roll multiplier should match", customMultiplier, actualMultiplier, 0.01);
        assertEquals("Checkpoint transactions should match", customCheckpointTxns, actualCheckpointTxns);
        assertEquals("Calculated threshold should match expected", expectedThreshold, calculatedThreshold);
    }

    @Test
    public void testEditLogAutoRollDefaultConfigurationValues() throws Exception {
        // Given: Default configuration (no custom values set)

        // When: Get default configuration values
        float defaultMultiplier = conf.getFloat(
            DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD,
            DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT);
        long defaultCheckpointTxns = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
        long defaultThreshold = (long) (defaultMultiplier * defaultCheckpointTxns);

        // Then: Verify that default values are correct
        assertEquals("Default edit log auto-roll multiplier should match", 
            DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT, 
            defaultMultiplier, 0.01);
        assertEquals("Default checkpoint transactions should match", 
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT, 
            defaultCheckpointTxns);
        assertEquals("Default calculated threshold should match expected", 
            (long)(DFSConfigKeys.DFS_NAMENODE_EDIT_LOG_AUTOROLL_MULTIPLIER_THRESHOLD_DEFAULT * 
                   DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT), 
            defaultThreshold);
    }
}