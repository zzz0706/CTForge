package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.HBaseClassTestRule;

import java.util.Arrays;
import java.util.List;

@Category(SmallTests.class)
public class TestCompactionConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionConfiguration.class);

  @Test
  public void testMinCompactSizeZeroAllowsAllFiles() throws Exception {
    // 1. Configuration as input
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY, 0L);

    // 2. Prepare the test conditions
    StoreConfigInformation storeConfigInfo = mock(StoreConfigInformation.class);
    when(storeConfigInfo.getMemStoreFlushSize()).thenReturn(134217728L);

    CompactionConfiguration comConf = new CompactionConfiguration(conf, storeConfigInfo);
    assertEquals(0L, comConf.getMinCompactSize());

    ExploringCompactionPolicy policy = new ExploringCompactionPolicy(conf, storeConfigInfo);

    // Prepare candidates with mixed sizes
    HStoreFile sf1 = mock(HStoreFile.class);
    HStoreFile sf2 = mock(HStoreFile.class);
    HStoreFile sf3 = mock(HStoreFile.class);

    // Provide non-null readers to avoid NPE
    org.apache.hadoop.hbase.regionserver.StoreFileReader reader = mock(org.apache.hadoop.hbase.regionserver.StoreFileReader.class);
    when(sf1.getReader()).thenReturn(reader);
    when(sf2.getReader()).thenReturn(reader);
    when(sf3.getReader()).thenReturn(reader);

    // Provide file sizes to satisfy compaction policy logic
    when(reader.getTotalUncompressedBytes()).thenReturn(1024L);

    List<HStoreFile> candidates = Arrays.asList(sf1, sf2, sf3);

    // 3. Test code
    List<HStoreFile> result = policy.applyCompactionPolicy(candidates, false, false, 2, 10);

    // 4. Code after testing
    // Since minCompactSize is 0, all candidates should be considered regardless of ratio
    assertEquals(3, result.size());
  }
}