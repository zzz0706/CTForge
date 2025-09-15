package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Time;
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
    // testLeaseExpiryMsDefaultValue
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
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
    // testLeaseExpiryMsCustomValue
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
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
    // testPruneIfExpiredUsesConfiguredLeaseExpiry
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testPruneIfExpiredUsesConfiguredLeaseExpiry() throws Exception {
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
        
        // Create a mock DatanodeDescriptor
        DatanodeDescriptor dn = mock(DatanodeDescriptor.class);
        when(dn.getDatanodeUuid()).thenReturn("test-dn-uuid");
        
        // Register the node
        long leaseId = manager.requestLease(dn);
        assertNotEquals("Lease should be issued", 0, leaseId);
        
        // Get the NodeData to manipulate its leaseTimeMs
        java.lang.reflect.Field nodesField = BlockReportLeaseManager.class.getDeclaredField("nodes");
        nodesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> nodes = 
            (java.util.Map<String, Object>) nodesField.get(manager);
        Object node = nodes.get("test-dn-uuid");
        assertNotNull("Node should be registered", node);
        
        // Set the lease time to be far in the past
        long oldTime = Time.monotonicNow() - (customLeaseExpiry + 500); // 500ms buffer
        java.lang.reflect.Field leaseTimeMsField = node.getClass().getDeclaredField("leaseTimeMs");
        leaseTimeMsField.setAccessible(true);
        leaseTimeMsField.setLong(node, oldTime);
        
        // Call checkLease which internally calls pruneIfExpired
        boolean result = manager.checkLease(dn, Time.monotonicNow(), leaseId);
        
        // Assert that the lease was considered expired
        assertFalse("Lease should be expired", result);
    }

    @Test
    // testPreconditionCheckOnLeaseExpiryConfig
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
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
    
    @Test
    // testPruneIfExpiredWithExpiredLease
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testPruneIfExpiredWithExpiredLease() throws Exception {
        // Initialize BlockReportLeaseManager with a specific leaseExpiryMs (e.g., 300000)
        long leaseExpiryMs = 300000L;
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, leaseExpiryMs);
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Create a mock DatanodeDescriptor
        DatanodeDescriptor dn = mock(DatanodeDescriptor.class);
        when(dn.getDatanodeUuid()).thenReturn("test-dn-uuid-2");
        
        // Request a lease to create a NodeData entry
        long leaseId = manager.requestLease(dn);
        assertNotEquals("Lease should be issued", 0, leaseId);
        
        // Get the NodeData to manipulate its leaseTimeMs
        java.lang.reflect.Field nodesField = BlockReportLeaseManager.class.getDeclaredField("nodes");
        nodesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, Object> nodes = 
            (java.util.Map<String, Object>) nodesField.get(manager);
        Object node = nodes.get("test-dn-uuid-2");
        assertNotNull("Node should be registered", node);
        
        // Set leaseTimeMs to a time far in the past (e.g., current time - 400000 ms)
        long oldLeaseTime = Time.monotonicNow() - 400000L;
        java.lang.reflect.Field leaseTimeMsField = node.getClass().getDeclaredField("leaseTimeMs");
        leaseTimeMsField.setAccessible(true);
        leaseTimeMsField.setLong(node, oldLeaseTime);
        
        // Call checkLease with a monotonicNowMs that is greater than leaseTimeMs + leaseExpiryMs
        long currentTime = Time.monotonicNow();
        assertTrue("Current time should be greater than lease expiry time", 
                   currentTime > (oldLeaseTime + leaseExpiryMs));
        
        boolean result = manager.checkLease(dn, currentTime, leaseId);
        
        // Assert that the method returns false (indicating the lease has expired)
        assertFalse("Lease should be expired", result);
    }
}