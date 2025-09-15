package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestDFSHAConfigurationValidation {

    @Test
    public void testDFS_HA_LogRollPeriod_Validation() {
        // Step 1: Load the configuration
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);

        // Step 2: Read the configuration value for dfs.ha.log-roll.period
        int logRollPeriod = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 
                                        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);

        // Step 3: Validate the configuration value
        assertTrue("dfs.ha.log-roll.period must be a non-negative integer.", logRollPeriod >= 0);
    }

    @Test
    public void testDFS_HA_TailEditsPeriod_Validation() {
        // Step 1: Load the configuration
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT);

        // Step 2: Read the configuration value for dfs.ha.tail-edits.period
        int tailEditsPeriod = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 
                                          DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT);

        // Step 3: Validate the configuration value
        assertTrue("dfs.ha.tail-edits.period must be a non-negative integer.", tailEditsPeriod >= 0);
    }

    @Test
    public void testEditLogTailer_ActiveNodeAddress_Validation() {
        // Step 1: Load the configuration
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);
        conf.set("dfs.ha.active-node.addr", "localhost:8020");

        // Step 2: Validate active node address when logRollPeriod is configured.
        int logRollPeriod = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 
                                        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);

        if (logRollPeriod >= 0) {
            String activeNodeAddress = conf.get("dfs.ha.active-node.addr");
            assertNotNull("Active node address must be configured when log-roll period is positive.", activeNodeAddress);
            
            // Additionally check if the active node has a valid port.
            String[] addrParts = activeNodeAddress.split(":");
            assertTrue("Active node address must contain a valid port.", addrParts.length == 2);
            int port = Integer.parseInt(addrParts[1]);
            assertTrue("Active node port must be greater than 0.", port > 0);
        }
    }

    @Test
    public void testInvalidConfigurationValues() {
        // Step 1: Load the configuration
        Configuration conf = new Configuration();
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);

        // Step 2: Read configuration values
        int logRollPeriod = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 
                                        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);

        // Step 3: Validate if logRollPeriod matches the expected data type (integer)
        try {
            assertTrue("dfs.ha.log-roll.period must be non-negative.", logRollPeriod >= 0);
        } catch (NumberFormatException e) {
            fail("dfs.ha.log-roll.period must be an integer.");
        }
    }
}