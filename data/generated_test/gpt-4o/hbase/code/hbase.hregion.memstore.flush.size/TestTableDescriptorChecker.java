package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

@Category(SmallTests.class)
public class TestTableDescriptorChecker {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestTableDescriptorChecker.class);

    private static final HBaseTestingUtility utility = new HBaseTestingUtility();

    @Test
    public void testSanityCheck_flushSizeBelowLimit() throws IOException {
        // Test code
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = utility.getConfiguration();
        long flushSizeLowerLimit = conf.getLong("hbase.hregion.memstore.flush.lowerlimit", 1024 * 1024L); // Correct configuration key

        // 2. Prepare the test conditions.
        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("testTable"));
        tableDescBuilder.setMemStoreFlushSize(flushSizeLowerLimit - 1); // Set below permissible limit
        TableDescriptor tableDescriptor = tableDescBuilder.build();

        try {
            // 3. Test code.
            TableDescriptorChecker.sanityCheck(conf, tableDescriptor);
        } catch (IllegalArgumentException e) {
            // 4. Code after testing: Assert that exception was thrown and validate its message.
            assert e.getMessage() != null && e.getMessage().contains("MEMSTORE_FLUSH_SIZE");
        }
    }
}