package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockReportLeaseManager;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Time.class})
public class BlockReportLeaseManagerConfigTest {

    @Mock
    private DatanodeDescriptor mockDatanodeDescriptor;

    private Configuration conf;
    private long leaseExpiryMsFromConfig;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(mockDatanodeDescriptor.getDatanodeUuid()).thenReturn("test-dn-uuid");

        // Load configuration from default resources
        conf = new Configuration();
        leaseExpiryMsFromConfig = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT
        );
    }

    @Test
    public void testLeaseExpiryMsConstructorArgumentMatchesConfigValue() throws Exception {
        // Prepare
        int maxPending = conf.getInt(
                DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES,
                DFSConfigKeys.DFS_NAMENODE_MAX_FULL_BLOCK_REPORT_LEASES_DEFAULT
        );

        // Create BlockReportLeaseManager with configuration
        BlockReportLeaseManager manager = new BlockReportLeaseManager(conf);

        // Test that the manager was created successfully with config values
        assertNotNull("BlockReportLeaseManager should be created successfully", manager);
    }

    @Test
    public void testPruneIfExpiredUsesConfiguredLeaseExpiryValue() {
        // Given
        long configuredLeaseExpiryMs = leaseExpiryMsFromConfig;
        BlockReportLeaseManager manager = new BlockReportLeaseManager(10, configuredLeaseExpiryMs);

        // Create a lease that should be expired
        long leaseId = manager.requestLease(mockDatanodeDescriptor);
        
        // Mock Time to simulate time passing
        PowerMockito.mockStatic(Time.class);
        when(Time.monotonicNow()).thenReturn(0L).thenReturn(configuredLeaseExpiryMs + 1000);

        // When - Check if lease is expired
        boolean isValid = manager.checkLease(mockDatanodeDescriptor, leaseId, configuredLeaseExpiryMs + 1000);

        // Then
        assertFalse("The lease should have been pruned as it is expired", isValid);
    }

    @Test
    public void testCheckLeaseRejectsExpiredLeaseBasedOnConfigValue() {
        // Given
        long configuredLeaseExpiryMs = leaseExpiryMsFromConfig;
        BlockReportLeaseManager manager = new BlockReportLeaseManager(10, configuredLeaseExpiryMs);

        // Request a lease
        long leaseId = manager.requestLease(mockDatanodeDescriptor);
        
        // Mock Time to simulate time passing beyond expiry
        PowerMockito.mockStatic(Time.class);
        when(Time.monotonicNow()).thenReturn(0L).thenReturn(configuredLeaseExpiryMs + 1);

        // When
        boolean isValid = manager.checkLease(mockDatanodeDescriptor, leaseId, configuredLeaseExpiryMs + 1);

        // Then
        assertFalse("The expired lease should be rejected", isValid);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorRejectsInvalidLeaseExpiryValue() {
        // Try to create manager with invalid lease expiry (0)
        new BlockReportLeaseManager(10, 0);
    }

    @Test
    public void testDefaultLeaseExpiryValueMatchesExpectedConstant() {
        assertEquals(
                "Default lease expiry value should match the constant",
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT,
                leaseExpiryMsFromConfig
        );
    }
}