package org.apache.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.conf.ConfigRedactor;
import org.junit.Assert;
import org.junit.Test;

public class TestConfigRedactor {
    // Test case for the 'redact' method to ensure sensitive keys are handled correctly
    @Test
    public void testRedactMethodForSensitiveKey() {
        // Step 1: Get configuration value using API
        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS,
                          ".*password.*,.*secret.*,.*credential.*");

        // Step 2: Prepare the input conditions for unit testing by creating a ConfigRedactor instance
        ConfigRedactor redactor = new ConfigRedactor(configuration);

        // Step 3: Test code to check redaction of sensitive keys
        String sensitiveKey = "user.password";
        String sensitiveValue = "password123";
        String result = redactor.redact(sensitiveKey, sensitiveValue);

        // Sensitive key should result in redacted text (update the expected redaction text)
        String expectedRedactedText = "<redacted>";
        Assert.assertEquals("Sensitive key should be redacted.", expectedRedactedText, result);

        // Verify non-sensitive case
        String nonSensitiveKey = "user.name";
        String nonSensitiveValue = "hadoop_user";
        String nonSensitiveResult = redactor.redact(nonSensitiveKey, nonSensitiveValue);

        // Non-sensitive key should not be redacted
        Assert.assertEquals("Non-sensitive key should not be redacted.", nonSensitiveValue, nonSensitiveResult);
    }

    // Another test case for 'configIsSensitive' to ensure private logic is exercised indirectly
    @Test
    public void testConfigIsSensitiveIndirectly() {
        // Step 1: Get configuration value using API
        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS,
                          ".*password.*,.*secret.*,.*credential.*");

        // Step 2: Prepare input conditions for testing the ConfigRedactor
        ConfigRedactor redactor = new ConfigRedactor(configuration);

        // Test for sensitive key using public method that calls the private method internally
        String sensitiveTestKey = "api.credential";
        String sensitiveTestValue = "key12345";

        // Step 3: Test public method to validate redaction (indirect test of 'configIsSensitive')
        String result = redactor.redact(sensitiveTestKey, sensitiveTestValue);

        // Sensitive key should be identified and redacted (update the expected redaction text)
        String expectedRedactedText = "<redacted>";
        Assert.assertEquals("Sensitive key check via public method failed.", expectedRedactedText, result);

        // Verification for a non-sensitive key
        String nonSensitiveTestKey = "api.url";
        String nonSensitiveTestValue = "http://localhost";

        String nonSensitiveResult = redactor.redact(nonSensitiveTestKey, nonSensitiveTestValue);

        // Non-sensitive key should not be redacted
        Assert.assertEquals("Non-sensitive key check via public method failed.", nonSensitiveTestValue, nonSensitiveResult);
    }
}