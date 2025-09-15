package org.apache.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigRedactor;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class TestConfigRedactorMultiLinePatterns {

    @Test
    public void testMultiLineCustomPatternsParsedCorrectly() throws Exception {
        // 1. Create a fresh Configuration object
        Configuration conf = new Configuration(false);

        // 2. Set multi-line patterns using newline separators
        conf.set("hadoop.security.sensitive-config-keys",
                "secret$\nnewlinePattern.*\nanotherSecret$");

        // 3. Instantiate ConfigRedactor under test
        ConfigRedactor redactor = new ConfigRedactor(conf);

        // 4. Reflectively access compiledPatterns to assert size and correctness
        Field patternsField = ConfigRedactor.class.getDeclaredField("compiledPatterns");
        patternsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Pattern> compiledPatterns = (List<Pattern>) patternsField.get(redactor);

        // 5. Assert expected size
        assertEquals(3, compiledPatterns.size());

        // 6. Assert each pattern is compiled correctly (order is preserved)
        assertEquals("secret$", compiledPatterns.get(0).pattern());
        assertEquals("newlinePattern.*", compiledPatterns.get(1).pattern());
        assertEquals("anotherSecret$", compiledPatterns.get(2).pattern());
    }
}