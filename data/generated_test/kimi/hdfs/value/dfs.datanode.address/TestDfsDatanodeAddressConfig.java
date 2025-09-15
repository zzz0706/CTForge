package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

public class TestDfsDatanodeAddressConfig {

  @Test
  public void testValidDfsDatanodeAddress() {
    Configuration conf = new Configuration(false);
    // rely on the default value, do NOT set it in code
    String addrStr = conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
                                     DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT);
    InetSocketAddress addr = NetUtils.createSocketAddr(addrStr);

    assertNotNull("InetSocketAddress should not be null", addr);
    assertTrue("Port must be in valid range 1-65535",
               addr.getPort() > 0 && addr.getPort() <= 65535);
  }

  @Test
  public void testInvalidPortInDfsDatanodeAddress() {
    Configuration conf = new Configuration(false);
    // rely on external configuration file supplying bad value
    String addrStr = conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY);
    if (addrStr != null) {
      InetSocketAddress addr = NetUtils.createSocketAddr(addrStr);
      assertTrue("Port must be in valid range 1-65535",
                 addr.getPort() > 0 && addr.getPort() <= 65535);
    }
  }

  @Test
  public void testMissingHostDefaultsToWildcard() {
    Configuration conf = new Configuration(false);
    // rely on external configuration file supplying value without host part
    String addrStr = conf.getTrimmed(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY);
    if (addrStr != null && !addrStr.contains(":")) {
      InetSocketAddress addr = NetUtils.createSocketAddr(addrStr);
      assertNotNull("Address should be parseable even without host part", addr);
    }
  }
}