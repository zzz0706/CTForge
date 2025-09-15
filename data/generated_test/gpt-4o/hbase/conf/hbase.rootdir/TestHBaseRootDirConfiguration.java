package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Unit test to validate the correctness of the hbase.rootdir configuration in HBase 2.2.2.
 */
@Category(SmallTests.class)
public class TestHBaseRootDirConfiguration {

    @ClassRule  // 注意这里修改为ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestHBaseRootDirConfiguration.class);

    private static Configuration config;

    @BeforeClass
    public static void setUp() {
        // Use HBase configuration API instead of hardcoded values
        config = new Configuration();
    }

    @Test
    public void testHBaseRootDirConfiguration() {
        // Retrieve the hbase.rootdir configuration
        String hbaseRootDir = config.get(HConstants.HBASE_DIR);
        System.out.println("hbaseRootDir: " + hbaseRootDir);
      
        // 1. Validate filesystem scheme
        boolean isValidScheme = hbaseRootDir.startsWith("file://") || hbaseRootDir.startsWith("hdfs://")
                || hbaseRootDir.startsWith("s3://") || hbaseRootDir.startsWith("viewfs://");
        assertTrue("hbase.rootdir should contain a valid filesystem scheme (e.g., hdfs://, file://, s3://)", isValidScheme);

        // 2. Ensure it's not a relative path
        assertFalse("hbase.rootdir should not be a relative path", hbaseRootDir.startsWith(".."));

        // 3. Check path structure
        boolean hasValidStructure = hbaseRootDir.length() > 10 && hbaseRootDir.contains("/");
        assertTrue("hbase.rootdir should have a valid path structure", hasValidStructure);

        // 4. Validate specific ending
        assertTrue("hbase.rootdir should end with /hbase", hbaseRootDir.endsWith("/hbase") || hbaseRootDir.endsWith("hbase"));
    }
}
