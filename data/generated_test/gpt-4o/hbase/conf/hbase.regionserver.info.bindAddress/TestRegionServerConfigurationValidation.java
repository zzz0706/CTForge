package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.InetAddress;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({RegionServerTests.class, SmallTests.class})
public class TestRegionServerConfigurationValidation {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestRegionServerConfigurationValidation.class);

    /**
     * Tests the validity of the configuration `hbase.regionserver.info.bindAddress`.
     * This method ensures that the configuration value is valid and satisfies constraints.
     */
    @Test
    public void testBindAddressConfiguration() throws IOException {
        // Prepare configuration
        Configuration conf = new Configuration();
        
        // Retrieve the `hbase.regionserver.info.bindAddress` value
        String bindAddress = conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");
        
        // Step 1: Validate if the bindAddress is a valid local address
        try {
            InetAddress address = InetAddress.getByName(bindAddress);
            assertTrue("The address specified in 'hbase.regionserver.info.bindAddress' must be valid.",
                Addressing.isLocalAddress(address));
        } catch (IllegalArgumentException e) {
            assertFalse("The address specified in 'hbase.regionserver.info.bindAddress' is invalid.",
                Addressing.isLocalAddress(InetAddress.getByName(bindAddress)));
        }
        
        // Step 2: Ensure 'hbase.regionserver.info.bindAddress' resolves properly
        try {
            InetAddress.getByName(bindAddress);
        } catch (Exception e) {
            throw new IOException("Failed to resolve the bind address: " + bindAddress);
        }
        
        // Step 3: Verify the default value and absence of hardcoded dependencies from other components
        if ("0.0.0.0".equals(bindAddress)) {
            assertTrue("Default bind address '0.0.0.0' should be valid.",
                Addressing.isLocalAddress(InetAddress.getByName(bindAddress)));
        }
    }
}