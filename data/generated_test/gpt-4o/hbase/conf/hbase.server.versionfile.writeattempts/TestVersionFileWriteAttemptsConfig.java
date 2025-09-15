package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for validating the configuration
 * "hbase.server.versionfile.writeattempts" in HBase 2.2.2.
 * The value must be a positive integer.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestVersionFileWriteAttemptsConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestVersionFileWriteAttemptsConfig.class);

    /**
     * Validate that hbase.server.versionfile.writeattempts is positive.
     */
    @Test
    public void testVersionFileWriteAttemptsConfiguration() {
        Configuration conf = HBaseConfiguration.create();

        String key = HConstants.VERSION_FILE_WRITE_ATTEMPTS;
        int defaultValue = HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS;
        int writeAttempts = conf.getInt(key, defaultValue);

        assertTrue("Configuration " + key +
                " must be a positive integer, but was: " + writeAttempts,
                writeAttempts > 0);
    }
}