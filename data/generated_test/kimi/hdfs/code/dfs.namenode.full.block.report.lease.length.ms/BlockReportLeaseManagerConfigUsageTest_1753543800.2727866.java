package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockReportLeaseManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BlockReportLeaseManagerConfigUsageTest {

    private Configuration conf;
    private DatanodeDescriptor mockDn;

    @Before
    public void setUp() {
        conf = new Configuration();
        mockDn = mock(DatanodeDescriptor.class);
        when(mockDn.getDatanodeUuid()).thenReturn("test-dn-uuid");
    }

    private long getLeaseExpiryMs(BlockReportLeaseManager manager) throws Exception {
        Field field = BlockReportLeaseManager.class.getDeclaredField("leaseExpiryMs");
        field.setAccessible(true);
        return field.getLong(manager);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCheckLeaseWithExpiredLease() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        long customLeaseExpiryMs = 5000L; // 5 seconds
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, customLeaseExpiryMs);
        
        // 2. Prepare the test conditions.
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Verify the configured value is properly set
        assertEquals(customLeaseExpiryMs, getLeaseExpiryMs(manager));
        
        // Register a DatanodeDescriptor and request a lease
        long leaseId = manager.requestLease(mockDn);
        assertTrue("Lease ID should be non-zero", leaseId != 0);
        
        // Simulate time passage such that the lease expires
        long monotonicNowMs = Time.monotonicNow() + customLeaseExpiryMs + 1000; // Expire by 1 second
        
        // 3. Test code - Call checkLease with the expired lease ID
        boolean result = manager.checkLease(mockDn, monotonicNowMs, leaseId);
        
        // 4. Code after testing - Assert that the method returns false
        assertFalse("checkLease should return false for an expired lease", result);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testPruneExpiredPendingUsesConfiguredLeaseExpiry() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        long customLeaseExpiryMs = 3000L; // 3 seconds
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, customLeaseExpiryMs);
        
        // 2. Prepare the test conditions.
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Verify the configured value is properly set
        assertEquals(customLeaseExpiryMs, getLeaseExpiryMs(manager));
        
        // Request a lease
        long leaseId = manager.requestLease(mockDn);
        assertTrue("Lease ID should be non-zero", leaseId != 0);
        
        // Simulate time passage to expire the lease
        long monotonicNowMs = Time.monotonicNow() + customLeaseExpiryMs + 1000;
        
        // 3. Test code - Call checkLease which internally calls pruneExpiredPending
        boolean result = manager.checkLease(mockDn, monotonicNowMs, leaseId);
        
        // 4. Code after testing
        assertFalse("Lease should be expired and pruned", result);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testRequestLeaseAndCheckLeaseWithinValidTime() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        long customLeaseExpiryMs = 10000L; // 10 seconds
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, customLeaseExpiryMs);
        
        // 2. Prepare the test conditions.
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Verify the configured value is properly set
        assertEquals(customLeaseExpiryMs, getLeaseExpiryMs(manager));
        
        // Request a lease
        long leaseId = manager.requestLease(mockDn);
        assertTrue("Lease ID should be non-zero", leaseId != 0);
        
        // Check the lease immediately (should be valid)
        long monotonicNowMs = Time.monotonicNow();
        
        // 3. Test code
        boolean result = manager.checkLease(mockDn, monotonicNowMs, leaseId);
        
        // 4. Code after testing
        assertTrue("Lease should be valid within expiry time", result);
    }

    @Test
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testMultipleLeasesWithDifferentExpiryTimes() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        long customLeaseExpiryMs = 7000L; // 7 seconds
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS, customLeaseExpiryMs);
        
        // 2. Prepare the test conditions.
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);
        
        // Verify the configured value is properly set
        assertEquals(customLeaseExpiryMs, getLeaseExpiryMs(manager));
        
        // Create multiple DNs
        DatanodeDescriptor dn1 = mock(DatanodeDescriptor.class);
        when(dn1.getDatanodeUuid()).thenReturn("dn-1");
        DatanodeDescriptor dn2 = mock(DatanodeDescriptor.class);
        when(dn2.getDatanodeUuid()).thenReturn("dn-2");
        
        // Request leases for both DNs
        long leaseId1 = manager.requestLease(dn1);
        long leaseId2 = manager.requestLease(dn2);
        
        // 3. Test code - Check both leases are valid initially
        long monotonicNowMs = Time.monotonicNow();
        assertTrue("Lease 1 should be valid", manager.checkLease(dn1, monotonicNowMs, leaseId1));
        assertTrue("Lease 2 should be valid", manager.checkLease(dn2, monotonicNowMs, leaseId2));
        
        // Expire the first lease
        monotonicNowMs += customLeaseExpiryMs + 1000;
        
        // Check leases again
        boolean lease1Valid = manager.checkLease(dn1, monotonicNowMs, leaseId1);
        boolean lease2Valid = manager.checkLease(dn2, monotonicNowMs, leaseId2);
        
        // 4. Code after testing
        assertFalse("Lease 1 should be expired", lease1Valid);
        assertFalse("Lease 2 should also be expired", lease2Valid);
    }
}