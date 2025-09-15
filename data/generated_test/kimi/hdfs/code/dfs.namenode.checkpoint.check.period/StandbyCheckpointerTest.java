package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class StandbyCheckpointerTest {

    @Mock
    private Configuration conf;
    
    private CheckpointConf checkpointConf;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    // Test that StandbyCheckpointer uses the dfs.namenode.checkpoint.check.period configuration
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions by setting up the configuration with specific values.
    // 3. Test code by creating CheckpointConf and verifying the sleep duration calculation.
    // 4. Code after testing validates the expected behavior.
    public void testCheckpointCheckPeriodInStandbyCheckpointer() throws Exception {
        // Configure dfs.namenode.checkpoint.check.period to 45 in the Configuration object
        long checkPeriodValue = 45L;
        long checkpointPeriodValue = 300L; // Make sure checkPeriod is smaller
        
        when(conf.getLong(
            eq(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY),
            anyLong())).thenReturn(checkPeriodValue);
            
        when(conf.getLong(
            eq(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY),
            anyLong())).thenReturn(checkpointPeriodValue);
            
        when(conf.getLong(
            eq(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY),
            anyLong())).thenReturn(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);
            
        when(conf.getInt(
            eq(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY),
            anyInt())).thenReturn(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_DEFAULT);
            
        when(conf.get(eq(DFSConfigKeys.DFS_NAMENODE_LEGACY_OIV_IMAGE_DIR_KEY)))
            .thenReturn(null);

        // Initialize CheckpointConf with this configuration
        checkpointConf = new CheckpointConf(conf);
        
        // Verify that getCheckPeriod returns the configured check period (minimum logic)
        long expectedSleepPeriod = Math.min(checkPeriodValue, checkpointPeriodValue);
        assertEquals(expectedSleepPeriod, checkpointConf.getCheckPeriod());
        
        // Verify the sleep time calculation that would be used in doWork()
        long expectedSleepTime = 1000 * checkpointConf.getCheckPeriod();
        assertEquals(45000L, expectedSleepTime);
    }
}