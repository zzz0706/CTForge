package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class EditLogTailerConfigTest {

    @Mock
    private FSNamesystem namesystem;

    @Mock
    private FSEditLog editLog;

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        
        conf = new Configuration(false);
        
        // Set up required HA configuration to avoid "Could not determine namespace id" error
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "mycluster");
        conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".mycluster", "nn1,nn2");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".mycluster.nn1", "localhost:8020");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".mycluster.nn2", "localhost:8021");
        
        // Set the current NameNode ID to resolve the "Could not determine own NN ID" error
        conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
        
        when(namesystem.getEditLog()).thenReturn(editLog);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testEditLogTailerSleepTimeZeroValue() {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values
        conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 0);
        
        // 2. Prepare the test conditions
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // 3. Test code - accessing private field through reflection
        long sleepTimeMs = getSleepTimeMs(tailer);
        
        // 4. Code after testing
        assertEquals("Sleep time should be 0 when dfs.ha.tail-edits.period is set to 0", 
                0L, sleepTimeMs);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testEditLogTailerSleepTimeDefaultValue() {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values
        // Not setting the value explicitly to test default behavior
        
        // 2. Prepare the test conditions
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // 3. Test code - accessing private field through reflection
        long sleepTimeMs = getSleepTimeMs(tailer);
        
        // 4. Code after testing
        long expected = DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT * 1000L;
        assertEquals("Sleep time should be default value when not explicitly configured", 
                expected, sleepTimeMs);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testEditLogTailerSleepTimeCustomValue() {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values
        int customValue = 45; // seconds
        conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, customValue);
        
        // 2. Prepare the test conditions
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // 3. Test code - accessing private field through reflection
        long sleepTimeMs = getSleepTimeMs(tailer);
        
        // 4. Code after testing
        assertEquals("Sleep time should be custom value in milliseconds", 
                customValue * 1000L, sleepTimeMs);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testEditLogTailerConfigurationPropagation() {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values
        int testValue = 15; // seconds
        conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, testValue);
        
        // 2. Prepare the test conditions
        int configValue = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
                DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT);
        
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // 3. Test code - accessing private field through reflection
        long sleepTimeMs = getSleepTimeMs(tailer);
        
        // 4. Code after testing
        assertEquals("Configuration value should match sleep time in milliseconds",
                configValue * 1000L, sleepTimeMs);
    }

    private long getSleepTimeMs(EditLogTailer tailer) {
        try {
            java.lang.reflect.Field field = EditLogTailer.class.getDeclaredField("sleepTimeMs");
            field.setAccessible(true);
            return field.getLong(tailer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access sleepTimeMs field", e);
        }
    }
}