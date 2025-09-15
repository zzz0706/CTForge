package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MasterTests.class, SmallTests.class})
public class TestClusterStatusPublisherConfiguration {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestClusterStatusPublisherConfiguration.class);

    /**
     * Test case to validate that the configuration hbase.status.multicast.address.port
     * satisfies its constraints and dependencies.
     */
    @Test
    public void testMulticastPortConfiguration() {
        Configuration conf = new Configuration();
        // Step 1: Retrieve the hbase.status.multicast.address.port configuration value
        int multicastPort = conf.getInt(HConstants.STATUS_MULTICAST_PORT, HConstants.DEFAULT_STATUS_MULTICAST_PORT);

        // Step 2: Validate that the port is within the allowed range
        // Ports range from 0 to 65535 (inclusive), with valid application ports typically above 1024
        try {
            assertTrue("Port value must be between 0 and 65535", multicastPort >= 0 && multicastPort <= 65535);
            assertTrue("Recommended port value should be greater than 1024", multicastPort > 1024);
        } catch (AssertionError e) {
            fail("Invalid multicast port configuration: " + e.getMessage());
        }
    }

    /**
     * Test case to validate that the multicast address configuration is resolvable.
     */
    @Test
    public void testMulticastAddressConfiguration() {
        Configuration conf = new Configuration();
        // Step 1: Retrieve the hbase.status.multicast.address value
        String multicastAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS, HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);

        // Step 2: Validate that the multicast address can be resolved
        try {
            InetAddress address = InetAddress.getByName(multicastAddress);
            assertTrue("Multicast address must be resolvable", address != null);
        } catch (UnknownHostException e) {
            fail("Multicast address is invalid or unresolvable: " + multicastAddress);
        }
    }
}