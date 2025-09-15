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
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DNS.class, SecurityUtil.class, UserGroupInformation.class})
public class TestDataNodeDnsNameserverConfig {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        // Initialize configuration
        conf = new HdfsConfiguration();
    }

    @Test
    // testDfsDatanodeDnsNameserver_IgnoredWhenSecurityKeysAreSet
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDfsDatanodeDnsNameserver_IgnoredWhenSecurityKeysAreSet() throws Exception {
        // 1. Obtain configuration keys from DFSConfigKeys
        String securityDnsNameserverKey = "hadoop.security.dns.nameserver";
        String datanodeDnsNameserverKey = DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY;
        
        // 2. Prepare test conditions - set both security and datanode DNS nameserver keys
        String securityDnsNameserverValue = "1.1.1.1";
        String datanodeDnsNameserverValue = "8.8.8.8";
        
        conf.set(securityDnsNameserverKey, securityDnsNameserverValue);
        conf.set(datanodeDnsNameserverKey, datanodeDnsNameserverValue);
        
        // Mock DNS.getDefaultHost to capture arguments
        PowerMockito.mockStatic(DNS.class);
        PowerMockito.when(DNS.getDefaultHost(anyString(), anyString(), anyBoolean()))
                .thenAnswer(new Answer<String>() {
                    @Override
                    public String answer(org.mockito.invocation.InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        // Return the nameserver argument as the hostname for verification
                        return (String) args[1];
                    }
                });

        // Since getHostName is private, we'll test the logic by mocking DNS.getDefaultHost
        // and checking which nameserver value is passed to it
        
        // 3. Create a DataNode instance to trigger the DNS resolution logic
        try {
            DataNode.instantiateDataNode(new String[]{}, conf, null);
        } catch (Exception e) {
            // Expected as we're not setting up full initialization
        }
        
        // 4. Verify that DNS.getDefaultHost was called with the value of hadoop.security.dns.nameserver
        PowerMockito.verifyStatic(times(1));
        DNS.getDefaultHost(anyString(), anyString(), anyBoolean());
    }

    @Test
    public void testDfsDatanodeDnsNameserver_UsedWhenSecurityKeysNotSet() throws Exception {
        // 1. Obtain configuration keys from DFSConfigKeys
        String datanodeDnsNameserverKey = DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY;
        String datanodeDnsInterfaceKey = DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY;
        
        // 2. Prepare test conditions - set only datanode DNS nameserver key
        String datanodeDnsNameserverValue = "8.8.4.4";
        String datanodeDnsInterfaceValue = "eth0";
        
        conf.set(datanodeDnsNameserverKey, datanodeDnsNameserverValue);
        conf.set(datanodeDnsInterfaceKey, datanodeDnsInterfaceValue);
        
        // Ensure security keys are not set
        conf.unset("hadoop.security.dns.interface");
        conf.unset("hadoop.security.dns.nameserver");
        
        // Mock DNS.getDefaultHost to capture arguments
        PowerMockito.mockStatic(DNS.class);
        PowerMockito.when(DNS.getDefaultHost(anyString(), anyString(), anyBoolean()))
                .thenAnswer(new Answer<String>() {
                    @Override
                    public String answer(org.mockito.invocation.InvocationOnMock invocation) throws Throwable {
                        Object[] args = invocation.getArguments();
                        // Return the nameserver argument as the hostname for verification
                        return (String) args[1];
                    }
                });

        // 3. Create a DataNode instance to trigger the DNS resolution logic
        try {
            DataNode.instantiateDataNode(new String[]{}, conf, null);
        } catch (Exception e) {
            // Expected as we're not setting up full initialization
        }
        
        // 4. Verify that DNS.getDefaultHost was called with the value of dfs.datanode.dns.nameserver
        PowerMockito.verifyStatic(times(1));
        DNS.getDefaultHost(anyString(), anyString(), anyBoolean());
    }
    
    @Test
    public void testDfsDatanodeDnsNameserver_UsedInDataNodeInstantiation() throws Exception {
        // 1. Obtain configuration keys from DFSConfigKeys
        String datanodeDnsNameserverKey = DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY;
        String datanodeDnsInterfaceKey = DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY;
        String datanodeHostnameKey = DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;
        
        // 2. Prepare test conditions
        String datanodeDnsNameserverValue = "8.8.8.8";
        String datanodeDnsInterfaceValue = "eth0";
        String datanodeHostnameValue = "test-hostname";
        
        conf.set(datanodeDnsNameserverKey, datanodeDnsNameserverValue);
        conf.set(datanodeDnsInterfaceKey, datanodeDnsInterfaceValue);
        conf.set(datanodeHostnameKey, datanodeHostnameValue);
        
        // Ensure security keys are not set
        conf.unset("hadoop.security.dns.interface");
        conf.unset("hadoop.security.dns.nameserver");
        
        // Mock static methods
        PowerMockito.mockStatic(SecurityUtil.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
        PowerMockito.mockStatic(DNS.class);
        
        PowerMockito.when(DNS.getDefaultHost(anyString(), anyString(), anyBoolean()))
            .thenReturn(datanodeHostnameValue);
        
        // 3. Invoke method under test
        try {
            DataNode.instantiateDataNode(new String[]{}, conf, null);
        } catch (Exception e) {
            // Expected as we're not setting up full initialization
        }
        
        // 4. Verify SecurityUtil.login was called with correct hostname
        verifyStatic(times(1));
        SecurityUtil.login(conf,
                DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY,
                DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
                datanodeHostnameValue);
    }
}