package org.apache.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigRedactorTest {
    
    // Test code
    // 1. Use API to retrieve configuration value, avoid hardcoding configuration values
    // 2. Prepare test conditions
    // 3. Write test logic
    // 4. Validate test results
  
    @Test
    public void testRedactMethodWithNonSensitiveKey() {
        // Retrieve default configuration
        Configuration conf = new Configuration();

        // Instantiate ConfigRedactor using the configuration
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // Non-sensitive key and its corresponding value
        String nonSensitiveKey = "nonSensitiveKey.example";
        String originalValue = "plainTextValue";

        // Invoke redact method and validate its behavior
        String result = redactor.redact(nonSensitiveKey, originalValue);

        // Check that the original value is returned for non-sensitive keys
        assertEquals(originalValue, result);
    }
}