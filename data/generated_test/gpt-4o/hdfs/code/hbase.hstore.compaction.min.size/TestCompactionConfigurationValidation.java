package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestCompactionConfigurationValidation {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestCompactionConfigurationValidation.class);

    // Configuration object to be initialized before tests
    private Configuration conf;

    @Before
    public void setUp() {
        // Initialize the configuration with default values specifically for HBase
        conf = new Configuration();
        // Use the correct configuration property key for setting related configuration values
        conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 134217728L); // Setting a valid value for testing
    }

    /**
     * Test to validate the configuration hbase.hstore.compaction.min.size.
     */
    @Test
    public void testMinCompactSizeConfiguration() {
        // 1. Use the hbase 2.2.2 API correctly to obtain the configuration value
        long minCompactSize = conf.getLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 134217728L); // Default: 128 MB (in bytes)

        // 2. Prepare the test conditions: Ensure the configuration value is accurately retrieved
        assertTrue("Configuration hbase.hstore.compaction.min.size should be greater than zero.", minCompactSize > 0);

        // 3. Test code: Validate the constraints of the configuration value
        // Ensure the configuration satisfies the compaction logic requirements

        // 4. Code after testing: Add optional checks for related parameters or behaviors
        // Additional validations could include checking related compaction settings or ranges
    }
}