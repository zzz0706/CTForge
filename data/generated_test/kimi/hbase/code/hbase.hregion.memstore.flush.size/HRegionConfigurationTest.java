package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class HRegionConfigurationTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(HRegionConfigurationTest.class);

  @Test
  public void testBlockingMemStoreSizeIsMultiplierTimesFlushSize() throws Exception {
    // 1. Configuration as input
    Configuration conf = HBaseConfiguration.create();

    // 2. Dynamic expected value calculation
    long flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
                                  TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE);
    long multiplier = conf.getLong(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER,
                                   HConstants.DEFAULT_HREGION_MEMSTORE_BLOCK_MULTIPLIER);
    long expectedBlockingMemStoreSize = flushSize * multiplier;

    // 3. Mock/stub external dependencies
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TableName.valueOf("testTable")).build();
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(regionInfo.getTable())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf"))
        .setMemStoreFlushSize(0) // force use of configuration
        .build();
    WAL wal = mock(WAL.class);
    RegionServerServices rsServices = mock(RegionServerServices.class);

    // Create a minimal rootDir so the HRegion constructor does not NPE
    java.nio.file.Path tmpDir = java.nio.file.Files.createTempDirectory("hbase-test");
    org.apache.hadoop.fs.Path rootDir = new org.apache.hadoop.fs.Path(tmpDir.toUri().toString());
    org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal(conf);
    fs.mkdirs(rootDir);

    HRegion region = HRegion.createHRegion(regionInfo, rootDir, conf, tableDescriptor, wal);

    // 4. Invoke the method under test
    region.setHTableSpecificConf();

    // 5. Assertions and verification
    Field blockingMemStoreSizeField = HRegion.class.getDeclaredField("blockingMemStoreSize");
    blockingMemStoreSizeField.setAccessible(true);
    long actualBlockingMemStoreSize = (Long) blockingMemStoreSizeField.get(region);
    assertEquals(expectedBlockingMemStoreSize, actualBlockingMemStoreSize);
  }
}