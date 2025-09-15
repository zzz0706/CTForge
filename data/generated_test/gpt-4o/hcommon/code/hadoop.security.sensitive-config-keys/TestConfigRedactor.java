package org.apache.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class TestConfigRedactor {

    @Test
    public void testConfigIsSensitiveWithMatchingPattern() throws Exception {
        // Step 1: Create a Configuration object and provide sensitive patterns since SensitiveConfigKeys is not available
        Configuration configuration = new Configuration();
        configuration.set("hadoop.security.sensitive-config-keys", ".*password.*,.*secret.*,.*credential.*");

        // Step 2: Instantiate a ConfigRedactor object
        ConfigRedactor redactor = new ConfigRedactor(configuration);

        // Step 3: Use reflection to access and invoke the private method configIsSensitive
        java.lang.reflect.Method method = ConfigRedactor.class.getDeclaredMethod("configIsSensitive", String.class);
        method.setAccessible(true);

        // Prepare a key that matches one of the sensitive patterns (e.g., "password")
        String testKey = "my.password";

        // Step 4: Invoke the private method and observe the result
        boolean isSensitive = (boolean) method.invoke(redactor, testKey);

        // Assert that the method identifies the key as sensitive
        Assert.assertTrue("The key should be identified as sensitive.", isSensitive);
    }
}