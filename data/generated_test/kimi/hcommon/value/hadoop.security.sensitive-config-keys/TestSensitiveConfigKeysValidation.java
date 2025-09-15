package org.apache.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.junit.Assert.*;

public class TestSensitiveConfigKeysValidation {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @After
    public void tearDown() {
        conf.clear();
    }

    /**
     * Validate that the default value of hadoop.security.sensitive-config-keys
     * compiles into valid regular expressions.
     */
    @Test
    public void testDefaultSensitiveRegexesAreValid() {
        String sensitiveRegexList = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS,
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT);

        String[] regexes = org.apache.hadoop.util.StringUtils.getTrimmedStrings(sensitiveRegexList);
        for (String regex : regexes) {
            try {
                Pattern.compile(regex);
            } catch (PatternSyntaxException e) {
                fail("Invalid regular expression in default sensitive-config-keys: " + regex);
            }
        }
    }

    /**
     * Validate that any custom value supplied for hadoop.security.sensitive-config-keys
     * is a comma-separated list of valid regular expressions.
     */
    @Test
    public void testCustomSensitiveRegexesAreValid() {
        String sensitiveRegexList = conf.get(
                CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS);
        if (sensitiveRegexList == null) {
            // nothing to validate
            return;
        }

        String[] regexes = org.apache.hadoop.util.StringUtils.getTrimmedStrings(sensitiveRegexList);
        for (String regex : regexes) {
            try {
                Pattern.compile(regex);
            } catch (PatternSyntaxException e) {
                fail("Invalid regular expression in sensitive-config-keys: " + regex);
            }
        }
    }

    /**
     * Validate that ConfigRedactor instantiation succeeds with the current
     * configuration, ensuring all patterns are syntactically valid.
     */
    @Test
    public void testConfigRedactorInitialization() {
        try {
            ConfigRedactor redactor = new ConfigRedactor(conf);
            assertNotNull(redactor);
        } catch (Exception e) {
            fail("ConfigRedactor initialization failed due to invalid configuration: " + e.getMessage());
        }
    }
}