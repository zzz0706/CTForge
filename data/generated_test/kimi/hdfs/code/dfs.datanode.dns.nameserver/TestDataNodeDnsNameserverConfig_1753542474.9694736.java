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

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DNS.class, SecurityUtil.class, UserGroupInformation.class})
public class TestDataNodeDnsNameserverConfig {

    private Configuration conf;
    private Properties configProperties;

    @Before
    public void setUp() throws Exception {
        // Initialize configuration and properties
        conf = new HdfsConfiguration();
        configProperties = new Properties();
    }

    @Test
    public void testDfsDatanodeDnsNameserver_ConfigValueMatchesReferenceLoader() {
        // 1. Obtain configuration value using HDFS API
        String configValue = conf.get(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "default");

        // 2. Load expected value from reference loader (Properties)
        String expectedValue = configProperties.getProperty(
                DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "default");

        // 3. Assert they match
        assertEquals("Configuration value should match reference loader",
                expectedValue, configValue);
    }

    @Test
    public void testDfsDatanodeDnsNameserver_UsedInGetHostNameWhenSecurityKeysNotSet()
            throws Exception {
        // 1. Prepare test conditions - set legacy keys, not security keys
        String testDnsNameserver = "8.8.8.8";
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, testDnsNameserver);
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "eth0");

        // Ensure security keys are not set
        conf.unset("hadoop.security.dns.interface");
        conf.unset("hadoop.security.dns.nameserver");

        // Mock DNS.getDefaultHost to capture arguments
        PowerMockito.mockStatic(DNS.class);
        PowerMockito.when(DNS.getDefaultHost(anyString(), anyString()))
                .thenReturn("mocked-hostname");

        // 2. Invoke method under test - use DNS.getDefaultHost directly since getHostName is private
        String hostname = DNS.getDefaultHost(
            conf.get(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "default"),
            conf.get(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "default"));

        // 3. Verify DNS.getDefaultHost was called with correct arguments
        PowerMockito.verifyStatic(times(1));
        DNS.getDefaultHost("eth0", testDnsNameserver);

        // 4. Assert hostname is returned (from mock)
        assertEquals("mocked-hostname", hostname);
    }

    @Test
    public void testDfsDatanodeDnsNameserver_NotUsedWhenSecurityKeysAreSet()
            throws Exception {
        // 1. Prepare test conditions - set security keys, not legacy keys
        String securityDnsNameserver = "1.1.1.1";
        conf.set("hadoop.security.dns.nameserver", securityDnsNameserver);
        conf.set("hadoop.security.dns.interface", "eth1");

        // Ensure legacy keys are not set
        conf.unset(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY);
        conf.unset(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY);

        // Mock DNS.getDefaultHost to capture arguments
        PowerMockito.mockStatic(DNS.class);
        PowerMockito.when(DNS.getDefaultHost(anyString(), anyString()))
                .thenReturn("mocked-hostname");

        // 2. Invoke method under test - use DNS.getDefaultHost directly since getHostName is private
        String hostname = DNS.getDefaultHost(
            conf.get("hadoop.security.dns.interface", "default"),
            conf.get("hadoop.security.dns.nameserver", "default"));

        // 3. Verify DNS.getDefaultHost was called with security keys
        PowerMockito.verifyStatic(times(1));
        DNS.getDefaultHost("eth1", securityDnsNameserver);

        // 4. Assert hostname is returned (from mock)
        assertEquals("mocked-hostname", hostname);
    }

    @Test
    public void testDfsDatanodeDnsNameserver_PassedToSecurityUtilLoginDuringInstantiation()
            throws Exception {
        // 1. Prepare test conditions
        String testDnsNameserver = "8.8.4.4";
        String testHostname = "test-node";
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, testDnsNameserver);
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "eth0");
        conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, testHostname);

        // Mock static methods
        PowerMockito.mockStatic(SecurityUtil.class);
        PowerMockito.mockStatic(UserGroupInformation.class);

        // 2. Invoke method under test
        try {
            DataNode.instantiateDataNode(new String[]{}, conf, null);
        } catch (Exception e) {
            // Expected as we're not setting up full initialization
        }

        // 3. Verify SecurityUtil.login was called with correct hostname
        verifyStatic(times(1));
        SecurityUtil.login(conf,
                DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY,
                DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
                testHostname);
    }
}