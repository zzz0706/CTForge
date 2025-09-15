package org.apache.hadoop.conf;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.regex.Pattern;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSensitiveConfigKeysValidation {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Obtain configuration values from the actual configuration files
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  /**
   * Validate that every regex listed under hadoop.security.sensitive-config-keys
   * is a syntactically valid regular expression.
   */
  @Test
  public void testSensitiveConfigKeysRegexValidity() {
    // 2. Prepare test conditions: read the runtime value
    String sensitiveRegexList = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS,
        CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS_DEFAULT);

    // 3. Test code: ensure each line/entry is a valid regex
    String[] regexes = org.apache.hadoop.util.StringUtils.getTrimmedStrings(sensitiveRegexList);
    for (String regex : regexes) {
      try {
        Pattern.compile(regex);
      } catch (java.util.regex.PatternSyntaxException e) {
        // 4. Fail the test if any regex is invalid
        assertFalse("Invalid regex in hadoop.security.sensitive-config-keys: " + regex, true);
      }
    }
  }
}