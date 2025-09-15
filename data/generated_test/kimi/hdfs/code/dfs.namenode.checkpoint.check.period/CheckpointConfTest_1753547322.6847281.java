package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CheckpointConfTest {

    private Configuration conf;
    private CheckpointConf checkpointConf;

    @Before
    public void setUp() {
        conf = new Configuration();
        checkpointConf = new CheckpointConf(conf);
    }

    @Test
    public void testCheckpointCheckPeriodDefaultValue() {
        // Ensure the default value is correctly loaded from DFSConfigKeys
        long expectedDefault = DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT;
        long actualValue = conf.getLong(
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY,
            DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT
        );
        assertEquals("Default value should match DFSConfigKeys constant", expectedDefault, actualValue);
    }

    @Test
    public void testGetCheckPeriodReturnsMinimumOfCheckPeriodAndCheckpointPeriod() {
        // Set up configuration values
        long checkPeriodValue = 30L;
        long checkpointPeriodValue = 60L;
        
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, checkPeriodValue);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, checkpointPeriodValue);

        CheckpointConf customConf = new CheckpointConf(conf);
        long result = customConf.getCheckPeriod();
        
        // Should return the minimum of the two
        assertEquals("getCheckPeriod should return the minimum of check periods", checkPeriodValue, result);
    }

    @Test
    public void testGetCheckPeriodReturnsCheckpointPeriodWhenCheckPeriodIsLarger() {
        // Set up configuration values where check period is larger than checkpoint period
        long checkPeriodValue = 60L;
        long checkpointPeriodValue = 30L;
        
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, checkPeriodValue);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, checkpointPeriodValue);

        CheckpointConf customConf = new CheckpointConf(conf);
        long result = customConf.getCheckPeriod();
        
        // Should return the minimum of the two (checkpoint period)
        assertEquals("getCheckPeriod should return the minimum of check periods", checkpointPeriodValue, result);
    }

    @Test
    public void testGetTxnCount() {
        // Set up configuration value
        long txnCountValue = 1000000L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, txnCountValue);

        CheckpointConf customConf = new CheckpointConf(conf);
        long result = customConf.getTxnCount();
        
        assertEquals("getTxnCount should return the configured transaction count", txnCountValue, result);
    }
}