package org.apache.hadoop.conf;

import org.apache.hadoop.conf.ConfigRedactor;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import java.util.regex.PatternSyntaxException;

public class ConfigRedactorTest {

    @Test(expected = PatternSyntaxException.class)
    public void testMalformedPatternThrowsPatternSyntaxException() {
        Configuration conf = new Configuration(false);
        conf.set("hadoop.security.sensitive-config-keys", "[invalidRegex");
        new ConfigRedactor(conf);
    }
}