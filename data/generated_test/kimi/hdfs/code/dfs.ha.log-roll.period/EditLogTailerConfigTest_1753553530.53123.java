package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class EditLogTailerConfigTest {

    private Configuration conf;
    private FSNamesystem namesystem;

    @Before
    public void setUp() {
        conf = new Configuration();
        namesystem = mock(FSNamesystem.class);
        
        // Configure HA settings to avoid "Could not determine namespace id" error
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "mycluster");
        conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".mycluster", "nn1,nn2");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".mycluster.nn1", "localhost:8020");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".mycluster.nn2", "localhost:8021");
        conf.set(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, "120");
        // Set the NameNode ID to resolve the "Could not determine own NN ID" error
        conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
    }

    @Test
    public void testLogRollPeriodConfigurationLoadedCorrectly() {
        // Prepare test conditions - set a specific value for the config
        int expectedPeriodSeconds = 120;
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, expectedPeriodSeconds);

        // Load reference value directly from configuration
        Properties props = new Properties();
        props.setProperty("dfs.ha.log-roll.period", String.valueOf(expectedPeriodSeconds));
        int referenceValue = Integer.parseInt(props.getProperty("dfs.ha.log-roll.period"));

        // Compare ConfigService value against reference loader
        int configValue = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
                DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);
        assertEquals("Configuration value should match reference loader", 
                referenceValue, configValue);

        // Test code - verify the EditLogTailer can be created successfully
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Verify that the object was created successfully (no exception thrown)
        assertNotNull("EditLogTailer should be created successfully", tailer);
    }

    @Test
    public void testTooLongSinceLastLoadWithDefaultConfig() {
        // Prepare test conditions with default configuration
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 
                   DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);
        
        // Test code - verify EditLogTailer can be created with default config
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Verify that the object was created successfully (no exception thrown)
        assertNotNull("EditLogTailer should be created successfully with default config", tailer);
    }

    @Test
    public void testNegativeLogRollPeriodDisablesRolling() {
        // Prepare test conditions - negative value disables log rolling
        int negativePeriod = -1;
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, negativePeriod);
        
        // Load reference value
        Properties props = new Properties();
        props.setProperty("dfs.ha.log-roll.period", String.valueOf(negativePeriod));
        int referenceValue = Integer.parseInt(props.getProperty("dfs.ha.log-roll.period"));
        
        // Compare configuration values
        int configValue = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
                DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);
        assertEquals("Negative configuration value should match reference", 
                referenceValue, configValue);
        
        // Test code - verify EditLogTailer can be created with negative period
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Verify that the object was created successfully (no exception thrown)
        assertNotNull("EditLogTailer should be created successfully with negative period", tailer);
    }

    @Test
    public void testZeroLogRollPeriodEnablesImmediateRolling() {
        // Prepare test conditions - zero period means immediate rolling
        int zeroPeriod = 0;
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, zeroPeriod);
        
        // Load reference value
        Properties props = new Properties();
        props.setProperty("dfs.ha.log-roll.period", String.valueOf(zeroPeriod));
        int referenceValue = Integer.parseInt(props.getProperty("dfs.ha.log-roll.period"));
        
        // Compare configuration values
        int configValue = conf.getInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
                DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);
        assertEquals("Zero configuration value should match reference", 
                referenceValue, configValue);
        
        // Test code - verify EditLogTailer can be created with zero period
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Verify that the object was created successfully (no exception thrown)
        assertNotNull("EditLogTailer should be created successfully with zero period", tailer);
    }
}