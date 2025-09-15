package org.apache.hadoop.conf;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class ConfigRedactorTest {

  @Test
  public void testCustomSensitivePatternsOverrideDefaults() throws Exception {
    // 1. Create a new Configuration object
    Configuration conf = new Configuration(false);

    // 2. Set a custom pattern list
    String customPatterns = "myCustomSecret$,anotherPattern.*";
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_SENSITIVE_CONFIG_KEYS, customPatterns);

    // 3. Instantiate ConfigRedactor
    ConfigRedactor redactor = new ConfigRedactor(conf);

    // 4. Inspect compiledPatterns and assert its size is 2
    Field field = ConfigRedactor.class.getDeclaredField("compiledPatterns");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<Pattern> compiledPatterns = (List<Pattern>) field.get(redactor);
    assertEquals("Expected exactly 2 patterns", 2, compiledPatterns.size());

    // 5. Assert that the patterns match "myCustomSecret$" and "anotherPattern.*" exactly
    String[] expectedPatterns = StringUtils.getTrimmedStrings(customPatterns);
    boolean foundFirst = false;
    boolean foundSecond = false;
    for (Pattern p : compiledPatterns) {
      if (p.toString().equals(expectedPatterns[0])) {
        foundFirst = true;
      } else if (p.toString().equals(expectedPatterns[1])) {
        foundSecond = true;
      }
    }
    assertTrue("Pattern 'myCustomSecret$' not found", foundFirst);
    assertTrue("Pattern 'anotherPattern.*' not found", foundSecond);
  }
}