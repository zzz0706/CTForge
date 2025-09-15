package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.net.NetUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDFSNameNodeHttpsAddressValidation {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf.clear();
  }

  @Test
  public void testDefaultHttpsAddressIsValid() throws Exception {
    // 1. Obtain the default value without setting anything.
    String addrStr = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT);

    // 2. Validate the default value.
    InetSocketAddress addr = NetUtils.createSocketAddr(addrStr);
    assertNotNull("Default https address should be parseable", addr);
    assertTrue("Port must be in valid range 1-65535",
               addr.getPort() > 0 && addr.getPort() <= 65535);
  }

  @Test
  public void testInvalidHttpsAddressFormat() {
    // 1. Inject an invalid address string.
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "invalid:format");

    // 2. Attempt to parse and expect failure.
    String addrStr = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    if (addrStr == null || addrStr.isEmpty()) {
      addrStr = DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT;
    }
    try {
      NetUtils.createSocketAddr(addrStr);
      fail("Expected IllegalArgumentException for malformed address");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  @Test
  public void testEmptyHttpsAddress() {
    // 1. Inject empty string.
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "");

    // 2. Parse and verify fallback behavior.
    String addrStr = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    if (addrStr == null || addrStr.isEmpty()) {
      addrStr = DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT;
    }
    assertEquals("Empty value should fallback to default",
                 DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT, addrStr);
    InetSocketAddress addr = NetUtils.createSocketAddr(addrStr);
    assertNotNull("Default fallback address should be parseable", addr);
  }

  @Test
  public void testHttpsAddressWithWildcardHost() throws Exception {
    // 1. Use wildcard host.
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "0.0.0.0:50470");

    // 2. Validate via DFSUtil#getInfoServer.
    InetSocketAddress dummyRpc = new InetSocketAddress("localhost", 8020);
    URI uri = DFSUtil.getInfoServer(dummyRpc, conf, "https");
    assertEquals("https", uri.getScheme());
    assertEquals("localhost", uri.getHost());
    assertEquals(50470, uri.getPort());
  }

  @Test
  public void testHttpsAddressPortOutOfRange() {
    // 1. Inject port out of range.
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:70000");

    // 2. Validate.
    String addrStr = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    if (addrStr == null || addrStr.isEmpty()) {
      addrStr = DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT;
    }
    try {
      NetUtils.createSocketAddr(addrStr);
      fail("Expected IllegalArgumentException for port out of range");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  @Test
  public void testHttpsAddressWithIPv6() {
    // 1. Inject IPv6 address.
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "[2001:db8::1]:50470");

    // 2. Validate.
    String addrStr = conf.getTrimmed(
        DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    if (addrStr == null || addrStr.isEmpty()) {
      addrStr = DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_DEFAULT;
    }
    InetSocketAddress addr = NetUtils.createSocketAddr(addrStr);
    assertNotNull("IPv6 address should be parseable", addr);
    assertEquals(50470, addr.getPort());
  }
}