package org.apache.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigRedactor;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestConfigRedactorEmptyPatternList {

    @Test
    public void testEmptyCustomPatternListLoadsNothing() throws Exception {
        // 1. Create a fresh Configuration and explicitly set the property to empty
        Configuration conf = new Configuration(false);
        conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS, "");

        // 2. Instantiate the system under test
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // 3. Obtain the private compiledPatterns list via reflection
        Field patternsField = ConfigRedactor.class.getDeclaredField("compiledPatterns");
        patternsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<java.util.regex.Pattern> compiledPatterns =
            (List<java.util.regex.Pattern>) patternsField.get(redactor);

        // 4. Assert that no patterns were loaded
        assertTrue("compiledPatterns should be empty when an empty string is provided",
                   compiledPatterns.isEmpty());
    }
}