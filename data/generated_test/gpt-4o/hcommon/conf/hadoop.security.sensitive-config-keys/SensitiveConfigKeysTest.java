package org.apache.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigRedactor;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class SensitiveConfigKeysTest {

    @Test
    public void testSensitiveConfigKeysConstraints() {
        Configuration configuration = new Configuration();

        // Retrieve configuration values using API (default value hardcoded due to missing class dependency)
        // Update logic due to missing SensitiveConfigKeys class from the source code
        String sensitiveRegexList = configuration.get(
                "hadoop.security.sensitive-config-keys",
                "(?i)password|secret|fs\\.s3\\.secretKey|oauth\\.token|ssl\\.keystore\\.pass");

        // Split into individual regexes and compile them into Patterns for validation
        List<String> sensitiveRegexes = Arrays.asList(sensitiveRegexList.split("\\s*,\\s*|\\s*\\n\\s*"));
        assertTrue("Sensitive regex list should not be empty", sensitiveRegexes.size() > 0);

        // Compile each regex and check for compilation success
        for (String regex : sensitiveRegexes) {
            try {
                Pattern.compile(regex);
            } catch (Exception e) {
                // Fail test in case of invalid regex
                assertFalse("Regex failed to compile: " + regex, true);
            }
        }
    }

    @Test
    public void testConfigIsSensitive() throws Exception {
        Configuration configuration = new Configuration();
        ConfigRedactor redactor = new ConfigRedactor(configuration);

        // Use reflection to access the private configIsSensitive method
        java.lang.reflect.Method configIsSensitiveMethod = ConfigRedactor.class.getDeclaredMethod("configIsSensitive", String.class);
        configIsSensitiveMethod.setAccessible(true);

        // Sensitive keys (to be matched against the regex patterns)
        assertTrue((boolean) configIsSensitiveMethod.invoke(redactor, "password"));
        assertTrue((boolean) configIsSensitiveMethod.invoke(redactor, "secret"));
        assertTrue((boolean) configIsSensitiveMethod.invoke(redactor, "fs.s3.secretKey"));
        assertTrue((boolean) configIsSensitiveMethod.invoke(redactor, "oauth.token"));
        assertTrue((boolean) configIsSensitiveMethod.invoke(redactor, "ssl.keystore.pass"));

        // Non-sensitive keys
        assertFalse((boolean) configIsSensitiveMethod.invoke(redactor, "nonSensitiveKey"));
        assertFalse((boolean) configIsSensitiveMethod.invoke(redactor, "userSetting"));
    }

    @Test
    public void testRedact() throws Exception {
        Configuration configuration = new Configuration();
        ConfigRedactor redactor = new ConfigRedactor(configuration);

        // Access the private constant "REDACTED_TEXT" using reflection
        java.lang.reflect.Field redactedTextField = ConfigRedactor.class.getDeclaredField("REDACTED_TEXT");
        redactedTextField.setAccessible(true);
        String redactedText = (String) redactedTextField.get(null);

        // Test redaction of sensitive values
        String sensitiveKey = "password";
        String sensitiveValue = "superSecretPassword";
        String redactedValue = redactor.redact(sensitiveKey, sensitiveValue);
        assertTrue("Value should be redacted", redactedValue.equals(redactedText));

        // Test that non-sensitive values are not redacted
        String nonSensitiveKey = "userSetting";
        String nonSensitiveValue = "regularValue";
        String returnedValue = redactor.redact(nonSensitiveKey, nonSensitiveValue);
        assertTrue("Value should not be redacted", returnedValue.equals(nonSensitiveValue));
    }
}