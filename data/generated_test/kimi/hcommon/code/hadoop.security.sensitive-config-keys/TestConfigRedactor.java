package org.apache.hadoop.conf;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.ConfigRedactor;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestConfigRedactor {

    @Test
    public void testRedactReturnsOriginalValueForNonSensitiveKey() {
        // 1. Instantiate Configuration (defaults are used)
        Configuration conf = new Configuration(false);

        // 2. Instantiate ConfigRedactor with default configuration
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // 3. Invoke the method under test
        String actual = redactor.redact("dfs.namenode.http.address", "localhost:50070");

        // 4. Assert the result
        assertEquals("localhost:50070", actual);
    }
}