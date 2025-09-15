package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionMemstoreBlockMultiplier {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestHRegionMemstoreBlockMultiplier.class);

  @Test
  public void verifyCustomMultiplierChangesBlockingSize() throws Exception {
    // 1. Configuration as Input
    Configuration conf = HBaseConfiguration.create();
    // 2. Dynamic Expected Value Calculation
    long multiplier = conf.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
                                   HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);
    long flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                                  TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
    long expectedBlocking = flushSize * multiplier;

    // 3. Prepare the test conditions
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TableName.valueOf("test"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    RegionInfo ri = RegionInfoBuilder.newBuilder(td.getTableName()).build();
    Path rootDir = new Path("file:///tmp/hbase-test");
    WALFactory walFactory = new WALFactory(conf, "test");
    WAL wal = walFactory.getWAL(ri);
    RegionServerServices rss = mock(RegionServerServices.class);

    // 4. Test code
    HRegion region = HRegion.createHRegion(ri, rootDir, conf, td, wal);
    region.setHTableSpecificConf();

    // 5. Code after testing
    assertEquals(expectedBlocking, region.getMemStoreFlushSize() * multiplier);
  }
}