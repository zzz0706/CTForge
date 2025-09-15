package org.apache.hadoop.hbase.conf;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.NetworkInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHBaseMulticastConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseMulticastConfig.class);

  @Test
  public void testHBaseStatusMulticastAddressIp() throws IOException {
    // Step 1: Prepare the test configuration.
    Configuration conf = new Configuration();

    // Step 2: Obtain the configuration value via HBase API.
    String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS, HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);

    // Step 3: Validate the configuration value against constraints.
    // Check if the value represents a valid IP address.
    try {
      InetAddress address = InetAddress.getByName(mcAddress);
      // Validate if the address is a multicast address.
      if (!address.isMulticastAddress()) {
        throw new IllegalArgumentException("The address " + mcAddress + " is not a valid multicast address.");
      }
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("The address " + mcAddress + " is not a valid IP address.", e);
    }
  }

  @Test
  public void testHBaseStatusMulticastDependencyValidation() throws IOException {
    // Step 1: Prepare the test configuration.
    Configuration conf = new Configuration();

    // Step 2: Obtain dependent configuration values via HBase API.
    String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS, HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);
    String bindAddress =
        conf.get(HConstants.STATUS_MULTICAST_BIND_ADDRESS, HConstants.DEFAULT_STATUS_MULTICAST_BIND_ADDRESS);
    int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT, HConstants.DEFAULT_STATUS_MULTICAST_PORT);
    String niName = conf.get(HConstants.STATUS_MULTICAST_NI_NAME);

    // Step 3: Validate the dependencies and configuration value.
    try {
      InetAddress multicastAddress = InetAddress.getByName(mcAddress);
      if (!multicastAddress.isMulticastAddress()) {
        throw new IllegalArgumentException("The address " + mcAddress + " is not a valid multicast address.");
      }

      if (bindAddress != null) {
        InetAddress bindInetAddress = InetAddress.getByName(bindAddress);
        if (bindInetAddress.isMulticastAddress()) {
          throw new IllegalArgumentException("The bind address " + bindAddress + " cannot be a multicast address.");
        }
      }

      if (port <= 0 || port > 65535) {
        throw new IllegalArgumentException("Invalid port number: " + port + ". Must be in the range 1-65535.");
      }

      if (niName != null) {
        NetworkInterface networkInterface = NetworkInterface.getByName(niName);
        if (networkInterface == null) {
          throw new IllegalArgumentException("The network interface " + niName + " is not valid or does not exist.");
        }
      }
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid IP address detected during dependency validation.", e);
    }
  }
}