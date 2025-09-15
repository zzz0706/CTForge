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
    // Test case: Verify behavior when passing a null configuration to `getInstance`.
    // 1. Use the HDFS 2.8.5 API correctly to trigger `getInstance` with null.
    // 2. Prepare the test condition by passing a null configuration object.
    // 3. Test code: Check for appropriate exception thrown.
    // 4. Code after testing: Ensure exception behavior matches expected results.
    public void test_getInstance_withNullConfiguration() {
        try {
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(null);
            fail("Expected a NullPointerException or similar error.");
        } catch (NullPointerException e) {
            // Verify the correct exception is thrown for null configuration input.
            assertEquals(NullPointerException.class, e.getClass());
        } catch (Exception e) {
            fail("Unexpected exception type: " + e.getClass().getName());
        }
    }

    @Test
    // Test case: Verify `getInstance` behavior with default configuration.
    // 1. Use the HDFS 2.8.5 API correctly to retrieve configuration values.
    // 2. Prepare test condition by using a default Configuration object.
    // 3. Test code: Validate the returned instance for correctness.
    // 4. Code after testing: Assert properties of the returned object.
    public void test_getInstance_withDefaultConfiguration() {
        Configuration conf = new Configuration();
        try {
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

            assertNotNull("Expected non-null TrustedChannelResolver instance.", resolver);
            assertEquals("Expected default TrustedChannelResolver class.", TrustedChannelResolver.class, resolver.getClass());
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    // Test case: Verify `getInstance` behavior with custom configuration for a resolver class.
    // 1. Use HDFS 2.8.5 API correctly to set custom resolver class in the configuration.
    // 2. Prepare test conditions with Configuration defining a custom resolver class.
    // 3. Test code: Validate the returned instance matches the custom configuration.
    // 4. Code after testing: Define/assert behavior of the returned custom instance.
    public void test_getInstance_withCustomConfiguration() {
        Configuration conf = new Configuration();
        conf.setClass(HdfsClientConfigKeys.DFS_TRUSTEDCHANNEL_RESOLVER_CLASS, CustomTrustedChannelResolver.class, TrustedChannelResolver.class);

        try {
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(conf);

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
        // Additional logic or overrides for testing can be implemented here.
    }
}