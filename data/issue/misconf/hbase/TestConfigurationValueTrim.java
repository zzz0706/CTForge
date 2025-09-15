package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

//HBASE-6973
@Category(SmallTests.class)
public class TestConfigurationValueTrim {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestConfigurationValueTrim.class);

    // Configuration key to test; use a key with a non-null default or site value
    private static final String CONF_KEY = "hbase.regionserver.keytab.file";

    /**
     * Test that the configured value for CONF_KEY is trimmed of
     * leading/trailing whitespace (i.e., value equals its trimmed form).
     */
    @Test
    public void testConfigValueTrimmed() {
        Configuration conf = HBaseConfiguration.create();

        // Retrieve the value from actual configuration
        String value = conf.get(CONF_KEY);
        assertNotNull("Configuration key '" + CONF_KEY + "' should be set", value);

        // The value should not have leading or trailing whitespace
        assertEquals(
            "Configuration value should be trimmed",
            value.trim(), value
        );
    }
}
