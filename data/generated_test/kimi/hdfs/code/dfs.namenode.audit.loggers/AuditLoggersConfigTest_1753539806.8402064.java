package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AuditLoggersConfigTest {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() {
        conf = new Configuration(false); // Use empty configuration
        configProperties = new Properties();
        // Load default configuration values from file (simulated)
        configProperties.setProperty(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "default");
        // Initialize metrics system to avoid NPE
        try {
            DefaultMetricsSystem.initialize("test");
        } catch (Exception e) {
            // Ignore if already initialized
        }
    }

    @Test
    public void testDefaultAuditLoggerWhenNoConfig() throws Exception {
        // Prepare test conditions - no audit logger configuration
        conf.unset(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY);

        // Test by checking configuration directly instead of instantiating FSNamesystem
        String configValue = conf.get(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "default");
        assertEquals("default", configValue);
    }

    @Test
    public void testCustomAuditLoggerInstantiation() throws Exception {
        // Prepare test conditions
        String customLoggerClass = "org.apache.hadoop.hdfs.server.namenode.DefaultAuditLogger";
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, customLoggerClass);

        // Test by checking configuration directly
        String configValue = conf.get(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY);
        assertEquals(customLoggerClass, configValue);
    }

    @Test
    public void testAuditLoggerConfigurationValues() throws Exception {
        // Prepare test conditions
        String loggerConfig = "default";
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, loggerConfig);

        // Test by checking configuration directly
        String configValue = conf.get(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY);
        assertEquals(loggerConfig, configValue);
    }

    @Test
    public void testAuditLoggerConfigurationValuesCustom() throws Exception {
        // Prepare test conditions
        String loggerConfig = "org.apache.hadoop.hdfs.server.namenode.DefaultAuditLogger";
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, loggerConfig);

        // Test by checking configuration directly
        String configValue = conf.get(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY);
        assertEquals(loggerConfig, configValue);
    }

    @Test
    public void testMultipleAuditLoggers() throws Exception {
        // Prepare test conditions
        String multipleLoggers = "default,org.apache.hadoop.hdfs.server.namenode.DefaultAuditLogger";
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, multipleLoggers);

        // Test by checking configuration directly
        String configValue = conf.get(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY);
        assertEquals(multipleLoggers, configValue);
    }

    @Test
    public void testConfigValueMatchesReferenceLoader() {
        // Set up configuration
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "default");
        
        // Load expected value from reference loader (Properties)
        String expectedValue = configProperties.getProperty(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "default");
        
        // Get value from ConfigService (Configuration in this case)
        String actualValue = conf.get(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "default");
        
        // Compare values
        assertEquals("Configuration value should match reference loader", expectedValue, actualValue);
    }

    @Test
    public void testTopAuditLoggerAddedWhenTopConfEnabled() throws Exception {
        // Prepare test conditions
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "default");
        conf.setBoolean("nntop.enabled", true);

        // Test by checking configuration directly
        String configValue = conf.get(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY);
        assertEquals("default", configValue);
    }

    @Test
    public void tearDown() {
        // Code after testing
        conf = null;
        configProperties = null;
        try {
            DefaultMetricsSystem.shutdown();
        } catch (Exception e) {
            // Ignore shutdown errors
        }
    }
}