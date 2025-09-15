package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCompactionMaxSizeConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionMaxSizeConfig.class);

  @Test
  public void testCustomMaxCompactSizeExcludesLargeSetInExploringPolicy() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_SIZE_KEY, 100L);

    // 2. Dynamic expected value calculation
    long expectedMaxCompactSize = conf.getLong(
        CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_SIZE_KEY, Long.MAX_VALUE);

    // 3. Mock/stub external dependencies
    StoreConfigInformation storeConf = mock(StoreConfigInformation.class);
    // StoreConfigInformation does not expose getConfiguration(); pass conf directly
    CompactionConfiguration cc = new CompactionConfiguration(conf, storeConf);
    assertEquals(expectedMaxCompactSize, cc.getMaxCompactSize());

    ExploringCompactionPolicy policy = new ExploringCompactionPolicy(conf, storeConf);

    // Build two mock files of 40 bytes each (eligible set sum = 80)
    HStoreFile f1 = mock(HStoreFile.class);
    StoreFileReader r1 = mock(StoreFileReader.class);
    when(r1.length()).thenReturn(40L);
    when(f1.getReader()).thenReturn(r1);
    when(f1.isReference()).thenReturn(false);

    HStoreFile f2 = mock(HStoreFile.class);
    StoreFileReader r2 = mock(StoreFileReader.class);
    when(r2.length()).thenReturn(40L);
    when(f2.getReader()).thenReturn(r2);
    when(f2.isReference()).thenReturn(false);

    // Build two mock files of 60 bytes each (ineligible set sum = 120)
    HStoreFile f3 = mock(HStoreFile.class);
    StoreFileReader r3 = mock(StoreFileReader.class);
    when(r3.length()).thenReturn(60L);
    when(f3.getReader()).thenReturn(r3);
    when(f3.isReference()).thenReturn(false);

    HStoreFile f4 = mock(HStoreFile.class);
    StoreFileReader r4 = mock(StoreFileReader.class);
    when(r4.length()).thenReturn(60L);
    when(f4.getReader()).thenReturn(r4);
    when(f4.isReference()).thenReturn(false);

    List<HStoreFile> candidates = Arrays.asList(f1, f2, f3, f4);

    // 4. Invoke the method under test
    List<HStoreFile> selected = policy.applyCompactionPolicy(
        candidates, false, false, 2, 10);

    // 5. Assertions and verification
    // Only the eligible 80-byte subset should be selected (f1 + f2)
    assertEquals(2, selected.size());
    assertEquals(80L, selected.stream().mapToLong(sf -> sf.getReader().length()).sum());
  }
}