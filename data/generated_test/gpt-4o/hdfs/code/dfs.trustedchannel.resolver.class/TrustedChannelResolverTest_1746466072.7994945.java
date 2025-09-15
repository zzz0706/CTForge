package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for TrustedChannelResolver class to ensure proper interaction with configuration values and instance creation.
 */
public class TrustedChannelResolverTest {

    @Test
    // Test case: Verify behavior of `getInstance` with a null configuration object.
    // 1. Use the HDFS 2.8.5 API correctly to test functionality.
    // 2. Prepare the test conditions by passing a null configuration object to `TrustedChannelResolver.getInstance`.
    // 3. Test code: Call `getInstance` with `null`.
    // 4. Code after testing: Validate exception behavior matches expected result or assert failure for unexpected behavior.
    public void test_getInstance_withNullConfiguration() {
        try {
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(null);
            fail("Expected a NullPointerException or similar error."); // Test fails if no exception is thrown.
        } catch (NullPointerException e) {
            // Verify expected exception is caught.
            assertEquals(NullPointerException.class, e.getClass());
        } catch (Exception e) {
            // Fail if any other exception is thrown.
            fail("Unexpected exception type: " + e.getClass().getName());
        }
    }

    @Test
    // Test case: Verify behavior of `getInstance` with default configuration.
    // 1. Use the HDFS 2.8.5 API correctly to interact with configuration.
    // 2. Prepare test conditions by using default configuration.
    // 3. Test code: Call `getInstance` using the configuration object.
    // 4. Code after testing: Validate return object matches expected default implementation type.
    public void test_getInstance_withDefaultConfiguration() {
        Configuration conf = new Configuration();
        try {
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

            // Assert non-null instance and validate class type matches default implementation.
            assertNotNull("Expected non-null TrustedChannelResolver instance.", resolver);
            assertEquals("Expected default TrustedChannelResolver class.", TrustedChannelResolver.class, resolver.getClass());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    // Test case: Verify behavior of `getInstance` with a custom-defined configuration for resolver class.
    // 1. Use HDFS 2.8.5 API for setting and resolving custom classes from configuration.
    // 2. Prepare test conditions by specifying a custom resolver class in the configuration.
    // 3. Test code: Verify behavior of `getInstance` with a configuration having the custom class.
    // 4. Code after testing: Ensure returned object matches expected custom class type.
    public void test_getInstance_withCustomConfiguration() {
        Configuration conf = new Configuration();
        conf.setClass(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, CustomTrustedChannelResolver.class, TrustedChannelResolver.class);

        try {
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

            // Assert non-null instance and validate class type matches custom implementation.
            assertNotNull("Expected non-null CustomTrustedChannelResolver instance.", resolver);
            assertEquals("Expected custom TrustedChannelResolver class.", CustomTrustedChannelResolver.class, resolver.getClass());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    /**
     * Custom implementation of TrustedChannelResolver for testing purposes.
     */
    public static class CustomTrustedChannelResolver extends TrustedChannelResolver {
        // Custom methods and overrides could be added here.
        // For this unit test, no overrides are added.
    }
}