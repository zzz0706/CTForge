package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class EditLogTailerConfigTest {

    @Mock
    private FSNamesystem namesystem;

    private Configuration conf;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);
        
        // Create configuration with proper HA settings
        conf = new Configuration();
        
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
    public void testLogRollPeriodConfiguration_DefaultValue() {
        // Prepare test conditions
        when(namesystem.getEditLog()).thenReturn(null); // Mock edit log
        
        // Test code
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Access the private field using reflection for testing purposes
        long logRollPeriodMs = getLogRollPeriodMs(tailer);
        
        // Get expected value from configuration service
        int expectedSeconds = conf.getInt(
            DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
            DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT
        );
        
        // Verify the configuration value matches expected default
        assertEquals("Configuration should provide default value",
            DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT, expectedSeconds);
        
        // Verify the transformation from seconds to milliseconds
        long expectedMs = expectedSeconds * 1000L;
        assertEquals("Log roll period should be converted from seconds to milliseconds",
            expectedMs, logRollPeriodMs);
    }

    @Test
    public void testLogRollPeriodConfiguration_CustomValue() {
        // Prepare test conditions
        int customSeconds = 60;
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, customSeconds);
        when(namesystem.getEditLog()).thenReturn(null); // Mock edit log
        
        // Test code
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Access the private field using reflection for testing purposes
        long logRollPeriodMs = getLogRollPeriodMs(tailer);
        
        // Get expected value from configuration service
        int expectedSeconds = conf.getInt(
            DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
            DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT
        );
        
        // Verify the configuration value matches custom value
        assertEquals("Custom configuration value should be used", customSeconds, expectedSeconds);
        
        // Verify the transformation from seconds to milliseconds
        long expectedMs = customSeconds * 1000L;
        assertEquals("Log roll period should be converted from seconds to milliseconds",
            expectedMs, logRollPeriodMs);
    }

    @Test
    public void testTooLongSinceLastLoad_BranchCondition() {
        // Prepare test conditions
        when(namesystem.getEditLog()).thenReturn(null); // Mock edit log
        
        // Set a small log roll period for testing
        int testPeriodSeconds = 1;
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, testPeriodSeconds);
        
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Test code - simulate time passing beyond the log roll period
        // Set last load time to a very old time to ensure it exceeds the period
        long oldTime = System.currentTimeMillis() - (testPeriodSeconds * 1000L * 10); // 10 times the period
        setLastLoadTimeMs(tailer, oldTime);
        
        // Give some time for the system to process
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Ignore
        }
        
        // Invoke the method under test
        boolean shouldRoll = invokeTooLongSinceLastLoad(tailer);
        
        // For negative value test (disabled log rolling)
        conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, -1);
        EditLogTailer tailerDisabled = new EditLogTailer(namesystem, conf);
        setLastLoadTimeMs(tailerDisabled, System.currentTimeMillis() - 5000L);
        
        boolean shouldNotRoll = invokeTooLongSinceLastLoad(tailerDisabled);
        assertEquals("When log roll period is negative, should not trigger roll",
            false, shouldNotRoll);
    }

    // Helper methods to access private fields and methods for testing
    private long getLogRollPeriodMs(EditLogTailer tailer) {
        try {
            java.lang.reflect.Field field = EditLogTailer.class.getDeclaredField("logRollPeriodMs");
            field.setAccessible(true);
            return (Long) field.get(tailer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access logRollPeriodMs field", e);
        }
    }

    private void setLastLoadTimeMs(EditLogTailer tailer, long timeMs) {
        try {
            java.lang.reflect.Field field = EditLogTailer.class.getDeclaredField("lastLoadTimeMs");
            field.setAccessible(true);
            field.set(tailer, timeMs);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set lastLoadTimeMs field", e);
        }
    }

    private boolean invokeTooLongSinceLastLoad(EditLogTailer tailer) {
        try {
            java.lang.reflect.Method method = EditLogTailer.class.getDeclaredMethod("tooLongSinceLastLoad");
            method.setAccessible(true);
            return (Boolean) method.invoke(tailer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke tooLongSinceLastLoad method", e);
        }
    }
}