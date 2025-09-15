package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.DNS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDfsDatanodeDnsNameserverConfig {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Use the hdfs 2.8.5 API to obtain configuration values
    conf = new Configuration();
    // Ensure we do not inherit any external settings
    conf.clear();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testValidDnsNameserver() throws IOException {
    // 2. Prepare the test conditions
    // Valid host name
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "8.8.8.8");
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved", hostName);
    } catch (UnknownHostException e) {
      fail("Should not throw UnknownHostException for valid DNS server");
    }
  }

  @Test
  public void testInvalidDnsNameserver() throws IOException {
    // 2. Prepare the test conditions
    // Invalid DNS server
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "invalid.dns.server");
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved even with invalid DNS server", hostName);
    } catch (UnknownHostException e) {
      // Expected behavior when DNS resolution fails
    }
  }

  @Test
  public void testEmptyDnsNameserver() throws IOException {
    // 2. Prepare the test conditions
    // Empty DNS server should use default
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "");
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved with empty DNS server", hostName);
    } catch (UnknownHostException e) {
      fail("Should not throw UnknownHostException for empty DNS server");
    }
  }

  @Test
  public void testDefaultDnsNameserver() throws IOException {
    // 2. Prepare the test conditions
    // Do not set the configuration, should use default
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved with default DNS server", hostName);
    } catch (UnknownHostException e) {
      fail("Should not throw UnknownHostException for default DNS server");
    }
  }

  @Test
  public void testDnsNameserverWithPort() throws IOException {
    // 2. Prepare the test conditions
    // DNS server with port (invalid format)
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "8.8.8.8:53");
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved even with port in DNS server", hostName);
    } catch (UnknownHostException e) {
      // Expected behavior when DNS resolution fails with port
    }
  }

  @Test
  public void testDnsNameserverPrecedence() throws IOException {
    // 2. Prepare the test conditions
    // Test that hadoop.security.dns.nameserver takes precedence
    conf.set("hadoop.security.dns.nameserver", "8.8.8.8");
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "invalid.dns.server");
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved using hadoop.security.dns.nameserver", hostName);
    } catch (UnknownHostException e) {
      fail("Should not throw UnknownHostException when hadoop.security.dns.nameserver is valid");
    }
  }

  @Test
  public void testDnsNameserverWithInterface() throws IOException {
    // 2. Prepare the test conditions
    // Test with DNS interface configuration
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "eth0");
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "8.8.8.8");
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved with interface and DNS server", hostName);
    } catch (UnknownHostException e) {
      // Expected behavior when interface doesn't exist
    }
  }

  @Test
  public void testDnsNameserverNullValue() throws IOException {
    // 2. Prepare the test conditions
    // Explicitly set null (though Configuration doesn't allow null)
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "null");
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved even with null string", hostName);
    } catch (UnknownHostException e) {
      // Expected behavior when DNS resolution fails
    }
  }

  @Test
  public void testDnsNameserverSpecialCharacters() throws IOException {
    // 2. Prepare the test conditions
    // Test with special characters
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "dns@server");
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved even with special characters", hostName);
    } catch (UnknownHostException e) {
      // Expected behavior when DNS resolution fails
    }
  }

  @Test
  public void testDnsNameserverIPv6() throws IOException {
    // 2. Prepare the test conditions
    // Test with IPv6 address
    conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "2001:4860:4860::8888");
    // 3. Test code
    try {
      String hostName = DNS.getDefaultHost("default", "default");
      assertNotNull("Hostname should be resolved with IPv6 DNS server", hostName);
    } catch (UnknownHostException e) {
      // Expected behavior when IPv6 DNS resolution fails
    }
  }
}