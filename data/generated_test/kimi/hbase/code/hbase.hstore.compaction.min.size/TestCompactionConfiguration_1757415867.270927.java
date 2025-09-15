package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCompactionConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionConfiguration.class);

  @Test
  public void testMinCompactSizeDefaultFromMemStoreFlushSize() {
    // 1. Configuration as input
    Configuration conf = new Configuration(); // do NOT set hbase.hstore.compaction.min.size

    // 2. Dynamic expected value calculation
    long expectedMemStoreFlushSize = 67108864L; // 64 MB in bytes

    // 3. Mock/Stub external dependencies
    StoreConfigInformation storeConfigInfo = mock(StoreConfigInformation.class);
    when(storeConfigInfo.getMemStoreFlushSize()).thenReturn(expectedMemStoreFlushSize);

    // 4. Invoke the method under test
    CompactionConfiguration compactionConf = new CompactionConfiguration(conf, storeConfigInfo);
    long actualMinCompactSize = compactionConf.getMinCompactSize();

    // 5. Assertions and verification
    assertEquals(expectedMemStoreFlushSize, actualMinCompactSize);
  }

  @Test
  public void testExploringCompactionPolicyUsesMinCompactSize() throws IOException {
    // 1. Configuration as input
    Configuration conf = new Configuration();
    // Explicitly set a small minCompactSize to ensure the ratio check is skipped
    long minCompactSize = 1024L;
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY, minCompactSize);

    // 2. Mock/Stub external dependencies
    StoreConfigInformation storeConfigInfo = mock(StoreConfigInformation.class);
    when(storeConfigInfo.getMemStoreFlushSize()).thenReturn(67108864L);
    CompactionConfiguration compactionConf = new CompactionConfiguration(conf, storeConfigInfo);

    // In HBase 2.2.2, ExploringCompactionPolicy has no public constructor that takes only CompactionConfiguration
    // Instead, create it via reflection or use the package-private constructor with additional parameters
    // Here we use the package-private constructor (Configuration, StoreConfigInformation)
    ExploringCompactionPolicy policy = new ExploringCompactionPolicy(conf, storeConfigInfo);

    // Create two small store files whose total size is below minCompactSize
    HStoreFile sf1 = mock(HStoreFile.class);
    HStoreFile sf2 = mock(HStoreFile.class);
    StoreFileReader reader1 = mock(StoreFileReader.class);
    StoreFileReader reader2 = mock(StoreFileReader.class);
    when(sf1.getReader()).thenReturn(reader1);
    when(sf2.getReader()).thenReturn(reader2);
    when(reader1.length()).thenReturn(500L);
    when(reader2.length()).thenReturn(500L);

    List<HStoreFile> candidates = new ArrayList<>();
    candidates.add(sf1);
    candidates.add(sf2);

    // 3. Invoke the method under test
    List<HStoreFile> selected = policy.applyCompactionPolicy(candidates, false, false, 2, 10);

    // 4. Assertions and verification
    assertEquals(2, selected.size());
    assertTrue(selected.contains(sf1));
    assertTrue(selected.contains(sf2));
  }

  @Test
  public void testRatioBasedCompactionPolicyUsesMinCompactSize() throws IOException {
    // 1. Configuration as input
    Configuration conf = new Configuration();
    // Explicitly set minCompactSize to a large value to force skipping large files
    long minCompactSize = 102400L;
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY, minCompactSize);

    // 2. Mock/Stub external dependencies
    StoreConfigInformation storeConfigInfo = mock(StoreConfigInformation.class);
    when(storeConfigInfo.getMemStoreFlushSize()).thenReturn(67108864L);
    CompactionConfiguration compactionConf = new CompactionConfiguration(conf, storeConfigInfo);

    // In HBase 2.2.2, RatioBasedCompactionPolicy has no public constructor that takes only CompactionConfiguration
    // Instead, create it via reflection or use the package-private constructor with additional parameters
    // Here we use the package-private constructor (Configuration, StoreConfigInformation)
    RatioBasedCompactionPolicy policy = new RatioBasedCompactionPolicy(conf, storeConfigInfo);

    // Create three store files: one large and two small
    HStoreFile sf1 = mock(HStoreFile.class);
    HStoreFile sf2 = mock(HStoreFile.class);
    HStoreFile sf3 = mock(HStoreFile.class);
    StoreFileReader reader1 = mock(StoreFileReader.class);
    StoreFileReader reader2 = mock(StoreFileReader.class);
    StoreFileReader reader3 = mock(StoreFileReader.class);
    when(sf1.getReader()).thenReturn(reader1);
    when(sf2.getReader()).thenReturn(reader2);
    when(sf3.getReader()).thenReturn(reader3);
    when(reader1.length()).thenReturn(200000L); // Larger than minCompactSize
    when(reader2.length()).thenReturn(1000L);
    when(reader3.length()).thenReturn(1000L);

    ArrayList<HStoreFile> candidates = new ArrayList<>();
    candidates.add(sf1);
    candidates.add(sf2);
    candidates.add(sf3);

    // 3. Invoke the method under test
    ArrayList<HStoreFile> selected = policy.applyCompactionPolicy(candidates, false, false);

    // 4. Assertions and verification
    // The large file should be skipped, leaving the two small files
    assertEquals(2, selected.size());
    assertTrue(selected.contains(sf2));
    assertTrue(selected.contains(sf3));
  }
}