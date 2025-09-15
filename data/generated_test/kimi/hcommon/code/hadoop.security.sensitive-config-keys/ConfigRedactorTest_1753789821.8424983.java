package org.apache.hadoop.conf;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigRedactor;
import org.junit.Test;

public class ConfigRedactorTest {

    @Test
    public void testRedactReturnsRedactedTextForSensitiveKey() {
        // 1. Instantiate Configuration using defaults
        Configuration conf = new Configuration(false);

        // 2. Instantiate ConfigRedactor with default configuration
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // 3. Invoke the method under test
        String actual = redactor.redact("dfs.ssl.keystore.password", "mypassword123");

        // 4. Compute expected redacted text dynamically
        String expected = "<redacted>";

        // 5. Assert the result
        assertEquals(expected, actual);
    }
}