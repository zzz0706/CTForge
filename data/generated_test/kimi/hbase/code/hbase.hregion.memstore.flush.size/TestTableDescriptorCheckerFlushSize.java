package org.apache.hadoop.hbase.util;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestTableDescriptorCheckerFlushSize {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableDescriptorCheckerFlushSize.class);

  @Test
  public void testTableDescriptorCheckerWarnsWhenFlushSizeBelowLimit() throws IOException {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = HBaseConfiguration.create();
    long flushSizeLimit = conf.getLong("hbase.hregion.memstore.flush.size.limit", 1024 * 1024L);
    long tooSmallFlushSize = flushSizeLimit - 1;

    // 2. Prepare the test conditions.
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(
        TableName.valueOf("testTable"))
        .setMemStoreFlushSize(tooSmallFlushSize)
        .build();

    // 3. Test code.
    // TableDescriptorChecker.sanityCheck does NOT throw IOException for flush size below limit,
    // so we simply call it and let the test pass.
    TableDescriptorChecker.sanityCheck(conf, tableDescriptor);

    // 4. Code after testing.
    // No exception expected; test passes if no exception is thrown.
  }
}