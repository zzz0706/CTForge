package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import static org.junit.Assert.assertTrue;

public class TestCheckpointConfParsingPropagation {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCheckpointConfParsingPropagation() {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, 60);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, 120);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 1000);

        // Instantiate CheckpointConf with the configuration
        CheckpointConf checkpointConf = new CheckpointConf(conf);

        // Test the parsing and propagation of configuration values
        long checkPeriod = checkpointConf.getCheckPeriod();
        
        // Ensure that getCheckPeriod() returns the minimum of the consistent checkpoint check period
        assertTrue("Parsed 'checkpointCheckPeriod' should be the minimum value between 'dfs.namenode.checkpoint.check.period' and 'dfs.namenode.checkpoint.period'",
                checkPeriod == Math.min(
                        conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY, DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT),
                        conf.getLong(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_KEY, DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT)
                ));
    }
}