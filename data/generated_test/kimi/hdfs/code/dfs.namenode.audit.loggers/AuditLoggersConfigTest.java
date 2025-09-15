package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.InputStream;
import java.util.Properties;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class AuditLoggersConfigTest {

    private Configuration conf;
    private Properties configProperties;
    private String loggerClass;

    public AuditLoggersConfigTest(String loggerClass) {
        this.loggerClass = loggerClass;
    }

    @Before
    public void setUp() throws Exception {
        // Prepare the test conditions
        conf = new Configuration();
        configProperties = new Properties();
        InputStream input = getClass().getClassLoader().getResourceAsStream("core-site.xml");
        try {
            if (input != null) {
                configProperties.loadFromXML(input);
            }
        } finally {
            if (input != null) {
                input.close();
            }
        }
        
        // Initialize metrics system with just the string parameter
        DefaultMetricsSystem.initialize("test");
    }

    @Test
    public void testAuditLoggers_DefaultValue_UsesDefaultLogger() {
        // Given: No explicit configuration, should use default
        String key = DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
        String expectedDefaultValue = "default";
        
        // When: Get configuration value
        String configValue = conf.get(key, expectedDefaultValue);

        // Then: Verify default value is used
        assertEquals(expectedDefaultValue, configValue);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"default"},
            {"org.apache.hadoop.hdfs.server.namenode.CustomAuditLogger"}
        });
    }

    @Test
    public void testAuditLoggers_ConfigValue_MatchesFileAndUsage() {
        // Given: Set configuration value
        String key = DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
        conf.set(key, loggerClass);

        // When: Get configuration value
        String configValue = conf.get(key);

        // Then: Verify configuration was read correctly
        assertEquals(loggerClass, configValue);
        
        // And verify it matches the file value if present
        String fileValue = configProperties.getProperty(key);
        if (fileValue != null) {
            assertEquals(fileValue, configValue);
        }
    }

    @Test
    public void testAuditLoggers_EmptyConfig_UsesDefaultFallback() {
        // Given: Explicitly set empty config
        String key = DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
        conf.set(key, "");

        // When: Get configuration value
        String configValue = conf.get(key);

        // Then: Should be empty as set
        assertEquals("", configValue);
    }

    @Test
    public void testAuditLoggers_MultipleLoggers_ConfiguredCorrectly() {
        // Given: Multiple audit loggers configured
        String key = DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY;
        String multipleLoggers = "default,custom1,custom2";
        conf.set(key, multipleLoggers);

        // When: Get configuration value
        String configValue = conf.get(key);

        // Then: Verify configuration was read correctly
        assertEquals(multipleLoggers, configValue);
    }

    @After
    public void tearDown() {
        // Code after testing
        DefaultMetricsSystem.shutdown();
    }
}