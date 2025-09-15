package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Properties;
import java.io.InputStream;
import java.io.IOException;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class EditLogTailerConfigTest {

    @Mock
    private FSNamesystem namesystem;

    @Mock
    private FSEditLog editLog;

    private Configuration conf;
    private Properties defaultProps;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        
        // Load default properties from Hadoop configuration files
        defaultProps = new Properties();
        try (InputStream input = this.getClass().getClassLoader().getResourceAsStream("hdfs-default.xml")) {
            if (input != null) {
                defaultProps.loadFromXML(input);
            }
        } catch (IOException e) {
            // If file not found, we'll use programmatic defaults
        }
        
        conf = new Configuration(false); // Don't load default resources automatically
        
        // Set up required HA configuration to avoid "Could not determine namespace id" error
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "mycluster");
        conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + ".mycluster", "nn1,nn2");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".mycluster.nn1", "localhost:8020");
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + ".mycluster.nn2", "localhost:8021");
        conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 
                 String.valueOf(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT));
        // Set the current NameNode ID to resolve the "Could not determine own NN ID" error
        conf.set(DFSConfigKeys.DFS_HA_NAMENODE_ID_KEY, "nn1");
        
        when(namesystem.getEditLog()).thenReturn(editLog);
    }

    @Test
    public void testDfsHaTailEditsPeriodDefaultValue() {
        // Prepare test conditions
        // No explicit configuration set, should use default
        
        // Test code
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Code after testing - verify sleepTimeMs is set correctly with default value
        int expectedDefault = DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT;
        // Accessing private field through reflection for testing purposes
        long actualSleepTimeMs = getSleepTimeMs(tailer);
        
        assertEquals("Sleep time should be default value in milliseconds", 
                expectedDefault * 1000L, actualSleepTimeMs);
        
        // Also verify against configuration file default if available
        String fileDefault = defaultProps.getProperty("dfs.ha.tail-edits.period");
        if (fileDefault != null) {
            assertEquals("Configuration file default should match coded default",
                    Integer.parseInt(fileDefault) * 1000L, actualSleepTimeMs);
        }
    }

    @Test
    public void testDfsHaTailEditsPeriodCustomValue() {
        // Prepare test conditions
        int customValue = 30; // seconds
        conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, customValue);
        
        // Test code
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Code after testing - verify sleepTimeMs is set correctly with custom value
        long actualSleepTimeMs = getSleepTimeMs(tailer);
        
        assertEquals("Sleep time should be custom value in milliseconds", 
                customValue * 1000L, actualSleepTimeMs);
        
        // Verify config service value matches what we set
        int configValue = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
                DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT);
        assertEquals("Config service should return the custom value", customValue, configValue);
    }

    @Test
    public void testDfsHaTailEditsPeriodZeroValue() {
        // Prepare test conditions
        int zeroValue = 0;
        conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, zeroValue);
        
        // Test code
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Code after testing - verify sleepTimeMs handles zero correctly
        long actualSleepTimeMs = getSleepTimeMs(tailer);
        
        assertEquals("Sleep time should be zero when configured as zero", 
                0L, actualSleepTimeMs);
    }

    @Test
    public void testDfsHaTailEditsPeriodMatchesConfigService() {
        // Prepare test conditions
        int testValue = 120;
        conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, testValue);
        
        // Test code
        int configValue = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
                DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT);
        EditLogTailer tailer = new EditLogTailer(namesystem, conf);
        
        // Code after testing - verify consistency between config service and usage
        long expectedSleepTimeMs = configValue * 1000L;
        long actualSleepTimeMs = getSleepTimeMs(tailer);
        
        assertEquals("EditLogTailer sleep time should match config service value (converted to ms)",
                expectedSleepTimeMs, actualSleepTimeMs);
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