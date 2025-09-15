package org.apache.hadoop.conf;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigRedactor;
import org.junit.Test;

public class ConfigRedactorTest {

    @Test
    public void testAzureAccountKeyPatternWildcard() {
        // 1. Instantiate Configuration; defaults are used, no explicit set
        Configuration conf = new Configuration();

        // 2. Build ConfigRedactor with the default Configuration
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // 3. Invoke method under test
        String redacted = redactor.redact(
                "fs.azure.account.key.mystorage.blob.core.windows.net",
                "mypassword");

        // 4. Assert the value is redacted
        assertEquals("<redacted>", redacted);
    }
}