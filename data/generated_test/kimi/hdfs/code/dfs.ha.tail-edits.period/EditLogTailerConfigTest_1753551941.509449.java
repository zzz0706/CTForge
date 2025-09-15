package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class EditLogTailerConfigTest {

    @Mock
    private FSNamesystem namesystem;

    @Mock
    private FSEditLog editLog;

    private Configuration conf;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        conf = new Configuration();
        // Configure required HA settings to avoid namespace ID error
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "mycluster");
        conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".mycluster", "nn1,nn2");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".mycluster.nn1", "localhost:8020");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".mycluster.nn2", "localhost:8021");
        conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, "mycluster");
        // Set the current NameNode ID to avoid "Could not determine own NN ID" error
        conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
        
        // Mock the edit log
        when(namesystem.getEditLog()).thenReturn(editLog);
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testEditLogTailerSleepTimeConfiguredValue() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
        int customPeriodSecs = 30;
        conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, customPeriodSecs);
        
        // 2. Prepare the test conditions
        // All necessary configuration has been set in the setup method
        
        // 3. Test code
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // 4. Code after testing - Use reflection to access the private sleepTimeMs field
        Field sleepTimeMsField = EditLogTailer.class.getDeclaredField("sleepTimeMs");
        sleepTimeMsField.setAccessible(true);
        long actualSleepTimeMs = sleepTimeMsField.getLong(tailer);
        
        // Verify that sleepTimeMs equals 30000 (30 seconds * 1000)
        assertEquals("The sleepTimeMs field in EditLogTailer should be set to 30000 milliseconds", 
                    30000L, actualSleepTimeMs);
    }

    @Test
    public void testEditLogTailerSleepTimeDefaultValue() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
        // Do not set the config value, let it use default
        
        // 2. Prepare the test conditions
        // All necessary configuration has been set in the setup method
        
        // 3. Test code
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // 4. Code after testing - Use reflection to access the private sleepTimeMs field
        Field sleepTimeMsField = EditLogTailer.class.getDeclaredField("sleepTimeMs");
        sleepTimeMsField.setAccessible(true);
        long actualSleepTimeMs = sleepTimeMsField.getLong(tailer);
        
        // Verify that sleepTimeMs equals 60000 (60 seconds * 1000) - the default value
        assertEquals("The sleepTimeMs field in EditLogTailer should be set to 60000 milliseconds by default", 
                    60000L, actualSleepTimeMs);
    }

    @Test
    public void testEditLogTailerSleepTimeZeroValue() throws Exception {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
        int customPeriodSecs = 0;
        conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, customPeriodSecs);
        
        // 2. Prepare the test conditions
        // All necessary configuration has been set in the setup method
        
        // 3. Test code
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // 4. Code after testing - Use reflection to access the private sleepTimeMs field
        Field sleepTimeMsField = EditLogTailer.class.getDeclaredField("sleepTimeMs");
        sleepTimeMsField.setAccessible(true);
        long actualSleepTimeMs = sleepTimeMsField.getLong(tailer);
        
        // Verify that sleepTimeMs equals 0 (0 seconds * 1000)
        assertEquals("The sleepTimeMs field in EditLogTailer should be set to 0 milliseconds", 
                    0L, actualSleepTimeMs);
    }
}