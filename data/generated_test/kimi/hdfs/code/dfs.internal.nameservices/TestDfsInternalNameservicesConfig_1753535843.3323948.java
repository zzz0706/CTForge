package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DFSUtil.class, DFSUtilClient.class})
public class TestDfsInternalNameservicesConfig {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @Test
    // testDfsInternalNameservicesUsedInGetNNServiceRpcAddressesForCluster
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDfsInternalNameservicesUsedInGetNNServiceRpcAddressesForCluster() throws Exception {
        // Prepare the test conditions
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2,ns3");
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns1", "nn1"), "host1:8020");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns2", "nn2"), "host2:8020");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns3", "nn3"), "host3:8020");

        // Mock DFSUtilClient.getAddressesForNsIds to return specific addresses
        PowerMockito.spy(DFSUtilClient.class);
        Map<String, Map<String, InetSocketAddress>> mockResult = new HashMap<>();
        Map<String, InetSocketAddress> ns1Addresses = new HashMap<>();
        ns1Addresses.put("nn1", new InetSocketAddress("host1", 8020));
        mockResult.put("ns1", ns1Addresses);
        
        PowerMockito.doReturn(mockResult)
                .when(DFSUtilClient.class, "getAddressesForNsIds", 
                      any(Configuration.class), 
                      any(Collection.class), 
                      anyString(), 
                      anyString(), 
                      anyString());

        // Test code
        Map<String, Map<String, InetSocketAddress>> result = DFSUtil.getNNServiceRpcAddressesForCluster(conf);

        // Assertions
        assertNotNull("Result should not be null", result);
        assertEquals("Result should contain only one nameservice", 1, result.size());
        assertTrue("Result should contain 'ns1'", result.containsKey("ns1"));
        assertFalse("Result should not contain 'ns2'", result.containsKey("ns2"));
        assertFalse("Result should not contain 'ns3'", result.containsKey("ns3"));
        
        // Verify that getAddressesForNsIds was called with the correct nameservice list
        PowerMockito.verifyStatic();
        DFSUtilClient.getAddressesForNsIds(any(Configuration.class), 
                                          org.mockito.Matchers.argThat(new org.hamcrest.BaseMatcher<Collection<String>>() {
                                              @Override
                                              public boolean matches(Object item) {
                                                  if (!(item instanceof Collection)) return false;
                                                  Collection<String> c = (Collection<String>) item;
                                                  return c.size() == 1 && c.contains("ns1");
                                              }

                                              @Override
                                              public void describeTo(org.hamcrest.Description description) {
                                                  description.appendText("Collection containing only ns1");
                                              }
                                          }), 
                                          anyString(), anyString(), anyString());
    }

    @Test
    public void testGetNNServiceRpcAddressesForCluster_UsesInternalNameservices() throws Exception {
        // Prepare the test conditions
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1,ns2");
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2,ns3");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns1"), "host1:8020");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns2"), "host2:8020");

        // Mock DFSUtilClient.getAddressesForNsIds to capture the argument
        PowerMockito.spy(DFSUtilClient.class);
        Map<String, Map<String, InetSocketAddress>> mockResult = new HashMap<>();
        mockResult.put("ns1", Collections.singletonMap("nn1", new InetSocketAddress("host1", 8020)));
        mockResult.put("ns2", Collections.singletonMap("nn2", new InetSocketAddress("host2", 8020)));
        
        // Replace lambda with anonymous inner class for Java 7 compatibility
        PowerMockito.doReturn(mockResult)
                .when(DFSUtilClient.class, "getAddressesForNsIds", any(Configuration.class),
                        any(Collection.class), anyString(), anyString(), anyString());

        // Test code
        Map<String, Map<String, InetSocketAddress>> result = DFSUtil.getNNServiceRpcAddressesForCluster(conf);

        // Assertions
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("ns1"));
        assertTrue(result.containsKey("ns2"));

        // Verify the internal nameservices were passed to getAddressesForNsIds
        PowerMockito.verifyStatic();
        DFSUtilClient.getAddressesForNsIds(eq(conf), 
                org.mockito.Matchers.argThat(new org.hamcrest.BaseMatcher<Collection<String>>() {
                    @Override
                    public boolean matches(Object item) {
                        if (!(item instanceof Collection)) {
                            return false;
                        }
                        Collection<String> c = (Collection<String>) item;
                        return c.contains("ns1") && c.contains("ns2") && c.size() == 2;
                    }

                    @Override
                    public void describeTo(org.hamcrest.Description description) {
                        description.appendText("Collection containing ns1 and ns2 only");
                    }
                }), 
                anyString(), anyString(), anyString());
    }

    @Test
    public void testGetNNServiceRpcAddressesForCluster_FallbackToNameservices() throws Exception {
        // Prepare the test conditions
        conf.unset(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns1"), "host1:8020");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns2"), "host2:8020");

        // Mock DFSUtilClient.getAddressesForNsIds
        PowerMockito.spy(DFSUtilClient.class);
        Map<String, Map<String, InetSocketAddress>> mockResult = new HashMap<>();
        mockResult.put("ns1", Collections.singletonMap("nn1", new InetSocketAddress("host1", 8020)));
        mockResult.put("ns2", Collections.singletonMap("nn2", new InetSocketAddress("host2", 8020)));
        
        // Replace lambda with anonymous inner class for Java 7 compatibility
        PowerMockito.doReturn(mockResult)
                .when(DFSUtilClient.class, "getAddressesForNsIds", any(Configuration.class),
                        any(Collection.class), anyString(), anyString(), anyString());

        // Test code
        Map<String, Map<String, InetSocketAddress>> result = DFSUtil.getNNServiceRpcAddressesForCluster(conf);

        // Assertions
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("ns1"));
        assertTrue(result.containsKey("ns2"));

        // Verify the nameservices were passed to getAddressesForNsIds
        PowerMockito.verifyStatic();
        DFSUtilClient.getAddressesForNsIds(eq(conf), 
                org.mockito.Matchers.argThat(new org.hamcrest.BaseMatcher<Collection<String>>() {
                    @Override
                    public boolean matches(Object item) {
                        if (!(item instanceof Collection)) {
                            return false;
                        }
                        Collection<String> c = (Collection<String>) item;
                        return c.contains("ns1") && c.contains("ns2") && c.size() == 2;
                    }

                    @Override
                    public void describeTo(org.hamcrest.Description description) {
                        description.appendText("Collection containing ns1 and ns2 only");
                    }
                }), 
                anyString(), anyString(), anyString());
    }

    @Test(expected = IOException.class)
    public void testGetNNServiceRpcAddressesForCluster_UnknownNameservice() throws IOException {
        // Prepare the test conditions
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "unknown_ns");
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2");

        // Test code - should throw IOException
        DFSUtil.getNNServiceRpcAddressesForCluster(conf);
    }

    @Test
    public void testGetNNLifelineRpcAddressesForCluster_UsesInternalNameservices() throws Exception {
        // Prepare the test conditions
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1,ns2");
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2,ns3");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY, "ns1"), "host1:8021");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_LIFELINE_RPC_ADDRESS_KEY, "ns2"), "host2:8021");

        // Mock DFSUtilClient.getAddressesForNsIds
        PowerMockito.spy(DFSUtilClient.class);
        Map<String, Map<String, InetSocketAddress>> mockResult = new HashMap<>();
        mockResult.put("ns1", Collections.singletonMap("nn1", new InetSocketAddress("host1", 8021)));
        mockResult.put("ns2", Collections.singletonMap("nn2", new InetSocketAddress("host2", 8021)));
        
        // Replace lambda with anonymous inner class for Java 7 compatibility
        PowerMockito.doReturn(mockResult)
                .when(DFSUtilClient.class, "getAddressesForNsIds", any(Configuration.class),
                        any(Collection.class), anyString(), anyString());

        // Test code
        Map<String, Map<String, InetSocketAddress>> result = DFSUtil.getNNLifelineRpcAddressesForCluster(conf);

        // Assertions
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("ns1"));
        assertTrue(result.containsKey("ns2"));

        // Verify the internal nameservices were passed to getAddressesForNsIds
        PowerMockito.verifyStatic();
        DFSUtilClient.getAddressesForNsIds(eq(conf), 
                org.mockito.Matchers.argThat(new org.hamcrest.BaseMatcher<Collection<String>>() {
                    @Override
                    public boolean matches(Object item) {
                        if (!(item instanceof Collection)) {
                            return false;
                        }
                        Collection<String> c = (Collection<String>) item;
                        return c.contains("ns1") && c.contains("ns2") && c.size() == 2;
                    }

                    @Override
                    public void describeTo(org.hamcrest.Description description) {
                        description.appendText("Collection containing ns1 and ns2 only");
                    }
                }), 
                anyString(), anyString());
    }

    @Test
    public void testGetInternalNameServices_UsesInternalNameservices() throws Exception {
        // Prepare the test conditions
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1,ns2");
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2,ns3");

        // Mock DFSUtilClient.getNameServiceIds for fallback scenario
        PowerMockito.spy(DFSUtilClient.class);
        List<String> nameServiceIds = Arrays.asList("ns1", "ns2", "ns3");
        PowerMockito.doReturn(nameServiceIds).when(DFSUtilClient.class, "getNameServiceIds", any(Configuration.class));

        // Test code
        Collection<String> result = DFSUtil.getInternalNameServices(conf);

        // Assertions
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains("ns1"));
        assertTrue(result.contains("ns2"));
    }

    @Test
    public void testGetInternalNameServices_FallbackToNameservices() throws Exception {
        // Prepare the test conditions
        conf.unset(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY);
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2,ns3");

        // Mock DFSUtilClient.getNameServiceIds
        PowerMockito.spy(DFSUtilClient.class);
        List<String> nameServiceIds = Arrays.asList("ns1", "ns2", "ns3");
        PowerMockito.doReturn(nameServiceIds).when(DFSUtilClient.class, "getNameServiceIds", any(Configuration.class));

        // Test code
        Collection<String> result = DFSUtil.getInternalNameServices(conf);

        // Assertions
        assertNotNull(result);
        assertEquals(3, result.size());
        assertTrue(result.contains("ns1"));
        assertTrue(result.contains("ns2"));
        assertTrue(result.contains("ns3"));
    }

    @Test
    public void testGetInternalNsRpcUris_UsesInternalNameservices() throws Exception {
        // Prepare the test conditions
        conf.set(DFSConfigKeys.DFS_INTERNAL_NAMESERVICES_KEY, "ns1,ns2");
        conf.set(DFSConfigKeys.DFS_NAMESERVICES, "ns1,ns2,ns3");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns1"), "host1:8020");
        conf.set(DFSUtil.addKeySuffixes(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY, "ns2"), "host2:8020");

        // Mock DFSUtil.getNameServiceUris to capture the argument
        PowerMockito.spy(DFSUtil.class);
        List<URI> mockUris = Arrays.asList(URI.create("hdfs://host1:8020"), URI.create("hdfs://host2:8020"));
        PowerMockito.doReturn(mockUris)
                .when(DFSUtil.class, "getNameServiceUris", any(Configuration.class),
                        any(Collection.class), anyString(), anyString());

        // Test code
        Collection<URI> result = DFSUtil.getInternalNsRpcUris(conf);

        // Assertions
        assertNotNull(result);
        assertEquals(2, result.size());
        assertTrue(result.contains(URI.create("hdfs://host1:8020")));
        assertTrue(result.contains(URI.create("hdfs://host2:8020")));

        // Verify the internal nameservices were passed to getNameServiceUris
        PowerMockito.verifyStatic();
        DFSUtil.getNameServiceUris(eq(conf), 
                org.mockito.Matchers.argThat(new org.hamcrest.BaseMatcher<Collection<String>>() {
                    @Override
                    public boolean matches(Object item) {
                        if (!(item instanceof Collection)) {
                            return false;
                        }
                        Collection<String> c = (Collection<String>) item;
                        return c.contains("ns1") && c.contains("ns2") && c.size() == 2;
                    }

                    @Override
                    public void describeTo(org.hamcrest.Description description) {
                        description.appendText("Collection containing ns1 and ns2 only");
                    }
                }), 
                anyString(), anyString());
    }
}