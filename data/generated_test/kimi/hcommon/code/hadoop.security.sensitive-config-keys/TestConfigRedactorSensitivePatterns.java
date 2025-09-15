package org.apache.hadoop.conf;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigRedactor;
import org.junit.Test;

public class TestConfigRedactorSensitivePatterns {

    @Test
    public void testCaseInsensitiveS3SecretKeyPattern() {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions.
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // 3. Test code.
        String redacted1 = redactor.redact("fs.s3.SecretKey", "AKIA...");
        assertEquals("<redacted>", redacted1);

        String redacted2 = redactor.redact("fs.s3.secret_key", "AKIA...");
        assertEquals("<redacted>", redacted2);

        // 4. Code after testing.
        // No additional cleanup needed; redactor is local to the test.
    }
}