package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class DatanodeManagerTest {

    private Configuration conf;
    private BlockManager blockManagerMock;
    private Namesystem namesystemMock;
    private CachedDNSToSwitchMapping dnsToSwitchMappingMock;

    @Before
    public void setUp() throws Exception {
        // 1. Initialize the configuration object
        conf = new Configuration();

        // 2. Mock dependencies using Mockito
        blockManagerMock = Mockito.mock(BlockManager.class);
        namesystemMock = Mockito.mock(Namesystem.class);
        dnsToSwitchMappingMock = Mockito.mock(CachedDNSToSwitchMapping.class);
    }

    @Test
    // Test case for DNS-to-switch mapping resolution with unresolvable hostnames
    // 1. Prepare the test conditions by mocking unresolvable includes and DNS mapping resolution.
    // 2. Verify the behavior of the system when unresolvable inputs are provided.
    public void testDnsToSwitchMappingWithUnresolvableIncludes() throws Exception {
        // 1. Prepare unresolvable InetSocketAddress objects
        InetSocketAddress unresolvableAddress1 = new InetSocketAddress("invalid-hostname-1", 1234);
        InetSocketAddress unresolvableAddress2 = new InetSocketAddress("invalid-hostname-2", 5678);

        // 2. Mock dnsToSwitchMapping to resolve input addresses to "unresolved"
        List<String> unresolvedResults = Arrays.asList("unresolved", "unresolved");
        when(dnsToSwitchMappingMock.resolve(anyList())).thenReturn(unresolvedResults);

        // 3. Create an instance of DatanodeManager by injecting mocked dependencies
        DatanodeManager datanodeManager = new DatanodeManager(blockManagerMock, namesystemMock, conf);

        // 4. Resolve the DNS-to-switch mapping
        List<String> resolvedLocations = dnsToSwitchMappingMock.resolve(
                Arrays.asList(
                        unresolvableAddress1.getHostName(),
                        unresolvableAddress2.getHostName()
                ));

        // 5. Assert the resolved locations match the mocked "unresolved" mapping
        assertEquals("Unresolved DNS mapping should return 'unresolved' results.", unresolvedResults, resolvedLocations);

        // 6. Verify that `resolve` was called with the expected arguments
        verify(dnsToSwitchMappingMock, times(1))
            .resolve(Arrays.asList(
                unresolvableAddress1.getHostName(), 
                unresolvableAddress2.getHostName()));
    }
}