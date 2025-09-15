package org.apache.hadoop.conf;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class ConfigRedactorTest {

    @Test
    public void testDefaultSensitivePatternsLoaded() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration(false);

        // 2. Dynamic expected value calculation
        String expectedRegexList = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT);
        String[] expectedPatterns = StringUtils.getTrimmedStrings(expectedRegexList);

        // 3. Invoke method under test
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // 4. Assertions and verification
        Field compiledPatternsField = ConfigRedactor.class.getDeclaredField("compiledPatterns");
        compiledPatternsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Pattern> actualCompiledPatterns = (List<Pattern>) compiledPatternsField.get(redactor);

        assertEquals("Number of compiled patterns should match default count",
                expectedPatterns.length, actualCompiledPatterns.size());

        for (String expected : expectedPatterns) {
            boolean found = false;
            for (Pattern actual : actualCompiledPatterns) {
                if (actual.pattern().equals(expected)) {
                    found = true;
                    break;
                }
            }
            assertTrue("Expected pattern not found: " + expected, found);
        }
    }
}