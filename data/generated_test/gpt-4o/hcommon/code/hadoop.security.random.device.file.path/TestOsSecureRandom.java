package org.apache.hadoop.crypto.random;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestOsSecureRandom {
    // Test code
    // 1. Use API to get configuration values, do not hardcode configuration values
    // 2. Prepare test conditions
    // 3. Test code
    // 4. Code after testing

    @Test
    public void test_nextBytes_validConfig() {
        // Step 1: Prepare test conditions
        Configuration conf = new Configuration(); // Get configuration values using API
        OsSecureRandom secureRandom = new OsSecureRandom(); // Initialize OsSecureRandom instance
        secureRandom.setConf(conf); // Set configuration for secureRandom

        try {
            // Step 3: Execute test code
            byte[] testBytes = new byte[256];
            secureRandom.nextBytes(testBytes); // Retrieve random bytes

            // Assert that the random bytes have been filled correctly
            boolean isAllZeroFilled = true; // Flag to check if all bytes are zero
            for (byte b : testBytes) {
                if (b != 0) {
                    isAllZeroFilled = false;
                    break;
                }
            }
            assertFalse("The byte array should not be filled with all zeroes", isAllZeroFilled);
        } catch (Exception e) {
            // Step 4: Handle exceptions and failures
            fail("Test failed due to exception: " + e.getMessage());
        }
    }
}