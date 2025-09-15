package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TrustedChannelResolverTest {

    @Test
    // Test case: Verify that `getInstance` properly handles a null configuration object by throwing a `NullPointerException` or similar defined behavior.
    // 1. Use the HDFS 2.8.5 API correctly to test functionality.
    // 2. Prepare the test conditions: Pass a null configuration object to `TrustedChannelResolver.getInstance`.
    // 3. Execute the test logic and capture exceptions or errors.
    // 4. Code after testing: Assert that the behavior matches the expected result.
    public void test_getInstance_withNullConfiguration() {
        try {
            // Test condition: Pass a `null` configuration.
            TrustedChannelResolver resolver = TrustedChannelResolver.getInstance(null);

            // If no exception is thrown, the test fails.
            assert false : "Expected an exception to be thrown when passing a null configuration.";
        } catch (NullPointerException e) {
            // Expected behavior: NullPointerException is thrown when configuration is null.
            assert true : "NullPointerException successfully caught.";
        } catch (Exception e) {
            // If another exception type is thrown, fail the test.
            assert false : "Unexpected exception type: " + e.getClass().getName();
        }
    }
}