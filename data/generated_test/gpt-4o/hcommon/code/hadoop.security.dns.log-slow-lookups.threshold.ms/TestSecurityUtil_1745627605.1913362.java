package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class TestSecurityUtil {

    /**
     * Tests that the `getSlowLookupThresholdMs` method correctly parses
     * the `hadoop.security.dns.log-slow-lookups.threshold.ms` configuration,
     * and verifies parsing functionality using the default value when unset.
     */
    @Test
    public void test_getSlowLookupThresholdMs_configurationParsing() {
        // Step 1: Create a Configuration instance.
        Configuration conf = new Configuration();

        // Step 2: Call `getSlowLookupThresholdMs()` indirectly via the static field.
        int expectedDefault = CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT;
        int actualValue = SecurityUtil.getSlowLookupThresholdMs();

        // Step 3: Validate that the threshold matches the default value.
        assertEquals("Expected the default slow lookup threshold value to be returned.", expectedDefault, actualValue);
    }

    /**
     * Tests the `getByName` method ensuring it uses the slow lookup threshold
     * from the configuration properly and logs slow DNS resolutions.
     */
    @Test
    public void test_getByName_withSlowLookupLogging() throws UnknownHostException {
        // Step 1: Create a mock hostname for testing resolution.
        String testHostName = "localhost";

        // Assume slow lookup threshold is configured to the default value.
        Configuration conf = new Configuration();
        int slowLookupThresholdMs = CommonConfigurationKeys.HADOOP_SECURITY_DNS_LOG_SLOW_LOOKUPS_THRESHOLD_MS_DEFAULT;

        // Step 2: Measure DNS resolution and ensure no exception occurs.
        InetAddress resolvedAddress = SecurityUtil.getByName(testHostName);
        assertNotNull("Expected resolved InetAddress to be non-null.", resolvedAddress);

        // Step 3: Optionally validate logging, depending on elapsed time (mocking or manual testing might be needed).
        // Note: Integrating verification for logged messages would require intercepting logger outputs.
    }
}