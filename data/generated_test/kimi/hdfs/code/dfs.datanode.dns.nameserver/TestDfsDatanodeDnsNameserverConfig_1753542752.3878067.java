package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SecurityUtil.class, DNS.class, UserGroupInformation.class, DataNode.class})
public class TestDfsDatanodeDnsNameserverConfig {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        conf = new HdfsConfiguration();
        PowerMockito.mockStatic(DNS.class);
        PowerMockito.mockStatic(SecurityUtil.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
    }

    @Test
    // testDfsDatanodeDnsNameserver_NotUsedWhenDatanodeHostnameSet
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDfsDatanodeDnsNameserver_NotUsedWhenDatanodeHostnameSet() throws Exception {
        // Prepare test conditions
        String customHostname = "custom-hostname.example.com";
        String dnsNameserver = "8.8.8.8";
        
        // Set dfs.datanode.hostname to a specific hostname value
        conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, customHostname);
        // Optionally set dfs.datanode.dns.nameserver to some value
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, dnsNameserver);
        
        // Mock DNS.getDefaultHost to verify it's never called
        when(DNS.getDefaultHost(anyString(), anyString(), anyBoolean()))
                .thenReturn("should-not-be-used");
        
        // Since getHostName is private, we'll test the behavior through DataNode instantiation
        // Mock SecurityUtil.login to avoid actual Kerberos login
        PowerMockito.doNothing().when(SecurityUtil.class, "login", 
                org.mockito.Matchers.any(Configuration.class), anyString(), anyString(), anyString());
        
        // Execute method under test
        try {
            DataNode.instantiateDataNode(new String[]{}, conf, null);
        } catch (Exception e) {
            // Expected as we're not setting up all required configurations
        }
        
        // Ensure DNS.getDefaultHost is never called when hostname is explicitly set
        verifyStatic(org.mockito.Mockito.times(0));
        DNS.getDefaultHost(anyString(), anyString(), anyBoolean());
    }

    @Test
    // testDfsDatanodeDnsNameserver_UsedWhenDatanodeHostnameNotSet
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDfsDatanodeDnsNameserver_UsedWhenDatanodeHostnameNotSet() throws Exception {
        // Prepare test conditions
        String dnsNameserver = "8.8.4.4";
        String dnsInterface = "default";
        String expectedHostname = "resolved-hostname.example.com";
        
        // Do not set dfs.datanode.hostname
        // Set dfs.datanode.dns.nameserver and dfs.datanode.dns.interface
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, dnsNameserver);
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, dnsInterface);
        
        // Mock DNS.getDefaultHost to return a specific value
        when(DNS.getDefaultHost(dnsInterface, dnsNameserver, false))
                .thenReturn(expectedHostname);
        
        // Mock SecurityUtil.login to avoid actual Kerberos login
        PowerMockito.doNothing().when(SecurityUtil.class, "login", 
                org.mockito.Matchers.any(Configuration.class), anyString(), anyString(), anyString());
        
        // Execute method under test
        try {
            DataNode.instantiateDataNode(new String[]{}, conf, null);
        } catch (Exception e) {
            // Expected as we're not setting up all required configurations
        }
        
        // Ensure DNS.getDefaultHost is called once with correct parameters
        verifyStatic(org.mockito.Mockito.times(1));
        DNS.getDefaultHost(eq(dnsInterface), eq(dnsNameserver), eq(false));
    }

    @Test
    // testDfsDatanodeDnsNameserver_UsedInDataNodeInstantiation
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDfsDatanodeDnsNameserver_UsedInDataNodeInstantiation() throws Exception {
        // Prepare test conditions
        String dnsNameserver = "1.1.1.1";
        String dnsInterface = "eth0";
        String expectedHostname = "instantiated-hostname.example.com";
        
        // Do not set dfs.datanode.hostname
        // Set dfs.datanode.dns.nameserver and dfs.datanode.dns.interface
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, dnsNameserver);
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, dnsInterface);
        
        // Mock DNS.getDefaultHost to return a specific value
        when(DNS.getDefaultHost(dnsInterface, dnsNameserver, false))
                .thenReturn(expectedHostname);
        
        // Mock SecurityUtil.login to avoid actual Kerberos login
        PowerMockito.doNothing().when(SecurityUtil.class, "login", 
                org.mockito.Matchers.any(Configuration.class), anyString(), anyString(), anyString());
        
        // Execute method under test
        try {
            DataNode.instantiateDataNode(new String[]{}, conf, null);
        } catch (Exception e) {
            // Expected as we're not setting up all required configurations
        }
        
        // Verify that DNS.getDefaultHost was called with correct parameters
        verifyStatic(org.mockito.Mockito.times(1));
        DNS.getDefaultHost(eq(dnsInterface), eq(dnsNameserver), eq(false));
        
        // Verify that SecurityUtil.login was called with the resolved hostname
        PowerMockito.verifyStatic(org.mockito.Mockito.times(1));
        SecurityUtil.login(org.mockito.Matchers.any(Configuration.class),
                          eq(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY),
                          eq(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY), 
                          eq(expectedHostname));
    }
}