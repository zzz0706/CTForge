package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SecurityUtil.class, DNS.class})
public class TestDfsDatanodeDnsInterfaceConfig {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        // Initialize configuration
        conf = new HdfsConfiguration();
        PowerMockito.mockStatic(SecurityUtil.class);
        PowerMockito.mockStatic(DNS.class);
    }

    // Helper method to call private DataNode.getHostName method using reflection
    private String callGetHostName(Configuration conf) throws Exception {
        Method method = DataNode.class.getDeclaredMethod("getHostName", Configuration.class);
        method.setAccessible(true);
        return (String) method.invoke(null, conf);
    }

    @Test
    public void testDfsDatanodeDnsInterfaceDefaultValue() throws Exception {
        // Prepare: Ensure no explicit hostname or security DNS interface is set
        conf.unset(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
        conf.unset("hadoop.security.dns.interface");
        conf.unset("hadoop.security.dns.nameserver");
        conf.unset(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY);

        // Set mock behavior for DNS resolution
        PowerMockito.when(DNS.getDefaultHost(isNull(String.class), anyString(), anyBoolean()))
                .thenReturn("mocked-hostname");

        // Test
        String hostname = callGetHostName(conf);

        // Verify that DNS.getDefaultHost was called with the correct parameters
        verifyStatic(times(1));
        DNS.getDefaultHost(
                conf.get(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY),
                conf.get(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "default"),
                false
        );

        // Assert that the returned hostname matches expected behavior
        assertEquals("mocked-hostname", hostname);
    }

    @Test
    public void testDfsDatanodeDnsInterfaceWithCustomValue() throws Exception {
        // Prepare: Set custom dfs.datanode.dns.interface value
        String customInterface = "eth2";
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, customInterface);
        conf.unset(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
        conf.unset("hadoop.security.dns.interface");
        conf.unset("hadoop.security.dns.nameserver");

        // Set mock behavior for DNS resolution
        PowerMockito.when(DNS.getDefaultHost(eq(customInterface), eq("default"), eq(false)))
                .thenReturn("custom-hostname");

        // Test
        String hostname = callGetHostName(conf);

        // Verify that DNS.getDefaultHost was called with the correct parameters
        verifyStatic(times(1));
        DNS.getDefaultHost(customInterface, "default", false);

        // Assert that the returned hostname matches expected behavior
        assertEquals("custom-hostname", hostname);
    }

    @Test
    public void testDfsDatanodeDnsInterfacePrecedenceOverLegacy() throws Exception {
        // Prepare: Set both hadoop.security.dns.interface (preferred) and dfs.datanode.dns.interface (legacy)
        String preferredInterface = "eth3";
        String legacyInterface = "eth2";
        conf.set("hadoop.security.dns.interface", preferredInterface);
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, legacyInterface);
        conf.unset(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
        conf.unset("hadoop.security.dns.nameserver");

        // Set mock behavior for DNS resolution - HDFS 2.8.5 uses null for nameserver when using security interface
        PowerMockito.when(DNS.getDefaultHost(eq(preferredInterface), (String) isNull(), eq(true)))
                .thenReturn("preferred-hostname");

        // Test
        String hostname = callGetHostName(conf);

        // Verify that DNS.getDefaultHost was called with the preferred parameters
        verifyStatic(times(1));
        DNS.getDefaultHost(preferredInterface, null, true);

        // Assert that the returned hostname matches expected behavior
        assertEquals("preferred-hostname", hostname);
    }

    @Test
    public void testDfsDatanodeDnsInterfaceInInstantiateDataNode() throws Exception {
        // Prepare: Mock static methods
        conf.unset(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
        conf.unset("hadoop.security.dns.interface");
        conf.unset("hadoop.security.dns.nameserver");
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "eth1");

        // Mock DNS resolution
        PowerMockito.when(DNS.getDefaultHost(eq("eth1"), eq("default"), eq(false)))
                .thenReturn("dn-hostname");

        // Call the method that should trigger DNS resolution
        String hostname = callGetHostName(conf);

        // Verify that DNS.getDefaultHost was called with the correct parameters
        verifyStatic(times(1));
        DNS.getDefaultHost("eth1", "default", false);

        // Assert that the returned hostname matches expected behavior
        assertEquals("dn-hostname", hostname);
    }
}