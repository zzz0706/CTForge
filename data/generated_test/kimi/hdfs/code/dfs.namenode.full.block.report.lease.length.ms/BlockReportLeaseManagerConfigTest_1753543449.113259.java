package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BlockReportLeaseManagerConfigTest {

    private Configuration conf;
    private Properties configProps;

    @Before
    public void setUp() throws IOException {
        conf = new Configuration();
        configProps = new Properties();
        configProps.load(this.getClass().getClassLoader().getResourceAsStream("core-default.xml"));
        configProps.load(this.getClass().getClassLoader().getResourceAsStream("hdfs-default.xml"));
    }

    @Test
    public void testLeaseExpiryMsDefaultValue() {
        // Verify that the configuration service returns the same value as the property file
        String configKey = DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS;
        long defaultValue = DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT;
        assertEquals("ConfigService and property file should have same default value",
                defaultValue, conf.getLong(configKey, defaultValue));

        // Verify constructor properly uses the config value by checking the actual object
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Use reflection to access private field for verification
        Long leaseExpiryMs = null;
        try {
            java.lang.reflect.Field field = BlockReportLeaseManager.class.getDeclaredField("leaseExpiryMs");
            field.setAccessible(true);
            leaseExpiryMs = (Long) field.get(manager);
        } catch (Exception e) {
            fail("Failed to access leaseExpiryMs field: " + e.getMessage());
        }
        
        assertEquals("leaseExpiryMs should be set from configuration default", defaultValue, leaseExpiryMs.longValue());
    }

    @Test
    public void testLeaseExpiryMsCustomValue() {
        // Set custom value in configuration
        String configKey = DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS;
        long customValue = 600000L;
        conf.setLong(configKey, customValue);

        // Verify that the configuration service returns the same value as what we set
        assertEquals("ConfigService should return the custom value",
                customValue, conf.getLong(configKey, -1L));

        // Verify constructor properly uses the custom config value
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Use reflection to access private field for verification
        Long leaseExpiryMs = null;
        try {
            java.lang.reflect.Field field = BlockReportLeaseManager.class.getDeclaredField("leaseExpiryMs");
            field.setAccessible(true);
            leaseExpiryMs = (Long) field.get(manager);
        } catch (Exception e) {
            fail("Failed to access leaseExpiryMs field: " + e.getMessage());
        }
        
        assertEquals("leaseExpiryMs should be set from configuration", customValue, leaseExpiryMs.longValue());
    }

    @Test
    public void testPruneIfExpiredUsesConfiguredLeaseExpiry() {
        // Set a small lease expiry for testing
        String configKey = DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS;
        long customLeaseExpiry = 1000L; // 1 second
        conf.setLong(configKey, customLeaseExpiry);

        // Create BlockReportLeaseManager with custom configuration
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Use reflection to access private field for verification
        Long leaseExpiryMs = null;
        try {
            java.lang.reflect.Field field = BlockReportLeaseManager.class.getDeclaredField("leaseExpiryMs");
            field.setAccessible(true);
            leaseExpiryMs = (Long) field.get(manager);
        } catch (Exception e) {
            fail("Failed to access leaseExpiryMs field: " + e.getMessage());
        }
        
        assertEquals("leaseExpiryMs should be set from configuration", customLeaseExpiry, leaseExpiryMs.longValue());
    }

    @Test
    public void testPreconditionCheckOnLeaseExpiryConfig() {
        // Test that invalid values are rejected
        String configKey = DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS;
        
        // Try with zero value - should throw IllegalArgumentException
        conf.setLong(configKey, 0L);
        try {
            new BlockReportLeaseManager(conf);
            fail("Should have thrown IllegalArgumentException for zero lease expiry");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Cannot set full block report lease expiry period to a value less than 1"));
        }

        // Try with negative value - should throw IllegalArgumentException
        conf.setLong(configKey, -1L);
        try {
            new BlockReportLeaseManager(conf);
            fail("Should have thrown IllegalArgumentException for negative lease expiry");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Cannot set full block report lease expiry period to a value less than 1"));
        }
    }
}