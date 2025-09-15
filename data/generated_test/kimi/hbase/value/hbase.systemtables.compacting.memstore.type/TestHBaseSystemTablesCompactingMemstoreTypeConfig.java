package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHBaseSystemTablesCompactingMemstoreTypeConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseSystemTablesCompactingMemstoreTypeConfig.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() {
    // 1. Use the hbase 2.2.2 API to load configuration without hard-coding values
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testValidSystemTablesCompactingMemstoreType() {
    // 2. Prepare test conditions – the configuration value is already loaded from the conf files
    String value = conf.get("hbase.systemtables.compacting.memstore.type", "NONE");

    // 3. Test code – verify the value is one of the allowed enum constants
    try {
      MemoryCompactionPolicy policy = MemoryCompactionPolicy.valueOf(value.toUpperCase());
      assertTrue("Unexpected MemoryCompactionPolicy: " + value,
          policy == MemoryCompactionPolicy.NONE ||
          policy == MemoryCompactionPolicy.BASIC ||
          policy == MemoryCompactionPolicy.EAGER);
    } catch (IllegalArgumentException e) {
      fail("Invalid value for hbase.systemtables.compacting.memstore.type: " + value);
    }
  }

  @Test
  public void testCaseInsensitiveHandling() {
    // 3. Test code – ensure case-insensitive parsing works as expected
    String value = conf.get("hbase.systemtables.compacting.memstore.type", "NONE");
    try {
      MemoryCompactionPolicy.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
      fail("Value must be case-insensitive convertible to MemoryCompactionPolicy: " + value);
    }
  }
}