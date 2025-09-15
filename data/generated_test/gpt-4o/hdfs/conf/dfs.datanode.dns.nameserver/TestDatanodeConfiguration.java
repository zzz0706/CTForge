package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDatanodeConfiguration {

  @Test
  public void testDfsDatanodeDnsNameserverConfiguration() {
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration config = new Configuration();

    // 2. Prepare the test conditions.
    // No additional setup required in this case.

    // 3. Test code.
    try {
      // Retrieve the value of the configuration
      String dnsNameserver = config.get(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "");

      // Validate the configuration value
      boolean isValid = isDnsNameServerValid(dnsNameserver);

      // Assert the validity of the configuration value
      assertTrue("The configuration value for dfs.datanode.dns.nameserver is invalid.", isValid);
    } catch (Exception e) {
      // Fail the test if an exception occurs
      fail("An error occurred while validating dfs.datanode.dns.nameserver configuration: " + e.getMessage());
    }

    // 4. Code after testing.
    // Cleanup or additional checks can be placed here if needed.
  }

  /**
   * Helper method to validate the dfs.datanode.dns.nameserver configuration.
   *
   * @param dnsNameserver the configuration value to validate
   * @return true if valid, false otherwise
   */
  private boolean isDnsNameServerValid(String dnsNameserver) {
    // Rule 1: dnsNameserver can be empty or "default", indicating fallback behavior
    if (dnsNameserver.isEmpty() || "default".equalsIgnoreCase(dnsNameserver)) {
      return true;
    }

    // Rule 2: Must be a valid hostname or IP address
    try {
      // Check if it is a valid hostname or IP using the InetAddress utility
      InetAddress.getByName(dnsNameserver);
      return true;
    } catch (Exception ex) {
      // Log if necessary (for debugging purposes)
    }

    return false;
  }
}