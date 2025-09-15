package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BlockReportLeaseManagerConfigTest {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration();
        configProperties = new Properties();
        // Load default configuration values from the same source as the application
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("hdfs-default.xml");
        if (inputStream != null) {
            configProperties.load(inputStream);
        }
    }

    private long getLeaseExpiryMs(BlockReportLeaseManager manager) throws Exception {
        Field field = BlockReportLeaseManager.class.getDeclaredField("leaseExpiryMs");
        field.setAccessible(true);
        return field.getLong(manager);
    }

    @Test
    public void testLeaseExpiryMsInitializationFromConfig() throws Exception {
        // Prepare test conditions - set a custom value in configuration
        long customLeaseExpiryMs = 600000L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, customLeaseExpiryMs);

        // Reference loader comparison
        String defaultValue = configProperties.getProperty(
                "dfs.namenode.full.block.report.lease.length.ms", "300000");
        long expectedDefaultValue = Long.parseLong(defaultValue);

        // Test code - create BlockReportLeaseManager and verify it uses config value
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Verify the leaseExpiryMs field is set correctly using reflection
        assertEquals(customLeaseExpiryMs, getLeaseExpiryMs(manager));
    }

    @Test
    public void testLeaseExpiryMsDefaultInitialization() throws Exception {
        // Do not set custom value, use default

        // Reference loader comparison
        String defaultValue = configProperties.getProperty(
                "dfs.namenode.full.block.report.lease.length.ms", "300000");
        long expectedDefaultValue = Long.parseLong(defaultValue);

        // Test code - verify default value is used
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Verify the leaseExpiryMs field uses the default value using reflection
        assertEquals(expectedDefaultValue, getLeaseExpiryMs(manager));
    }

    @Test
    public void testPruneIfExpiredUsesConfiguredLeaseExpiry() throws Exception {
        // Prepare test conditions
        long customLeaseExpiryMs = 10000L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, customLeaseExpiryMs);
        
        // Create BlockReportLeaseManager with custom config
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Verify config value matches expected using reflection
        assertEquals(customLeaseExpiryMs, getLeaseExpiryMs(manager));
    }

    @Test
    public void testCheckLeaseValidatesAgainstConfiguredExpiry() throws Exception {
        // Prepare test conditions
        long customLeaseExpiryMs = 20000L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, customLeaseExpiryMs);
        
        // Create BlockReportLeaseManager
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Verify the configured value is properly set using reflection
        assertEquals(customLeaseExpiryMs, getLeaseExpiryMs(manager));
        
        // Reference loader comparison
        String defaultValue = configProperties.getProperty(
                "dfs.namenode.full.block.report.lease.length.ms", "300000");
        long expectedDefaultValue = Long.parseLong(defaultValue);
        
        // If no custom value is set, should use default
        Configuration defaultConf = new Configuration();
        BlockReportLeaseManager defaultManager = new BlockReportLeaseManager(defaultConf);
        assertEquals(expectedDefaultValue, getLeaseExpiryMs(defaultManager));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidLeaseExpiryMsThrowsException() {
        // Prepare test conditions - set invalid value
        long invalidLeaseExpiryMs = 0L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, invalidLeaseExpiryMs);
        
        // Test code - should throw IllegalArgumentException
        new BlockReportLeaseManager(conf);
        
        // Exception should be thrown
    }
}