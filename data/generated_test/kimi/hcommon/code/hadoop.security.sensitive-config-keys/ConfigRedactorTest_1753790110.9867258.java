package org.apache.hadoop.conf;

import org.apache.hadoop.conf.ConfigRedactor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigRedactorTest {

    @Test
    public void testRecursiveSelfReferencePatternIncluded() {
        // 1. Use default configuration without explicit set
        Configuration conf = new Configuration();

        // 2. Instantiate ConfigRedactor with default Configuration
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // 3. Call redact with the recursive key
        String actual = redactor.redact(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS,
                "customValue");

        // 4. Assert the value is redacted
        assertEquals("<redacted>", actual);
    }
}