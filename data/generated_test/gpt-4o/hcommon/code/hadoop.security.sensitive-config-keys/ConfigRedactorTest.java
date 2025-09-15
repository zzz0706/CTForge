package org.apache.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ConfigRedactorTest {
    // Test code

    @Test
    public void testRedactMethodWithSensitiveKey() {
        // Create a Configuration object
        Configuration conf = new Configuration();

        // Instantiate a ConfigRedactor object using the created Configuration instance
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // Use a key that matches one of the sensitive patterns
        String sensitiveKey = "fs.azure.account.key.myaccount";
        String sensitiveValue = "SecretValue123";

        // Test the redact method with the sensitive key
        String result = redactor.redact(sensitiveKey, sensitiveValue);

        // Verify that the sensitive value is redacted
        // Use reflection to access the private field REDACTED_TEXT for verification
        try {
            java.lang.reflect.Field field = ConfigRedactor.class.getDeclaredField("REDACTED_TEXT");
            field.setAccessible(true);
            String redactedText = (String) field.get(redactor);

            assert result.equals(redactedText) : "Sensitive key value should be redacted.";
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Error accessing REDACTED_TEXT field in ConfigRedactor.", e);
        }
    }
}