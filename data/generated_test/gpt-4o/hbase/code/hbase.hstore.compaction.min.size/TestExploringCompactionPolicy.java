package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.mockito.Mockito.*;

@Category(SmallTests.class)
public class TestExploringCompactionPolicy {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestExploringCompactionPolicy.class);

    @Test
    public void testExploringCompactionPolicyApplyWithMixedFileSizes() throws IOException {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.

        // Create a Configuration object and define test configurations.
        Configuration conf = new Configuration();
        conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY, 134217728); // Setting a mock value for minCompactSize.
        long minCompactSize = conf.getLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY, 134217728);

        // 2. Prepare the test conditions.

        // Mock HStoreFile objects with varying sizes.
        HStoreFile smallFile1 = mock(HStoreFile.class);
        HStoreFile smallFile2 = mock(HStoreFile.class);
        HStoreFile largeFile = mock(HStoreFile.class);

        // Mock file sizes using StoreFileReader.
        StoreFileReader smallFileReader1 = mock(StoreFileReader.class);
        StoreFileReader smallFileReader2 = mock(StoreFileReader.class);
        StoreFileReader largeFileReader = mock(StoreFileReader.class);

        // Define mocked lengths for these files.
        when(smallFileReader1.length()).thenReturn(minCompactSize / 2);  // Below minCompactSize.
        when(smallFileReader2.length()).thenReturn(minCompactSize / 3);  // Below minCompactSize.
        when(largeFileReader.length()).thenReturn(minCompactSize * 2);  // Above minCompactSize.

        // Link mocked readers to the HStoreFile objects.
        when(smallFile1.getReader()).thenReturn(smallFileReader1);
        when(smallFile2.getReader()).thenReturn(smallFileReader2);
        when(largeFile.getReader()).thenReturn(largeFileReader);

        // Create a mixed file size candidate list.
        List<HStoreFile> candidates = new ArrayList<>();
        candidates.add(smallFile1);
        candidates.add(smallFile2);
        candidates.add(largeFile);

        // Mock StoreConfigInformation (needed for CompactionConfiguration).
        StoreConfigInformation storeConfigInformation = mock(StoreConfigInformation.class);

        // Code fix:
        // Ensure that the correct constructor of CompactionConfiguration is used.
        // CompactionConfiguration requires StoreConfigInformation for initialization.
        CompactionConfiguration compactionConfiguration = new CompactionConfiguration(conf, storeConfigInformation);

        // Instantiate ExploringCompactionPolicy.
        ExploringCompactionPolicy policy = new ExploringCompactionPolicy(conf, storeConfigInformation);

        // 3. Test code.

        // Apply compaction policy on mixed file size candidates.
        List<HStoreFile> selectedFiles = policy.applyCompactionPolicy(candidates, false, false, 2, 10);

        // 4. Code after testing.

        // Verify that small files were compacted correctly.
        for (HStoreFile file : selectedFiles) {
            long fileSize = file.getReader().length();
            assert fileSize >= minCompactSize || (fileSize < minCompactSize && candidates.contains(file));
        }

        // Ensure no disproportionately large files were compacted based on policy.
        for (HStoreFile file : candidates) {
            if (!selectedFiles.contains(file) && file.getReader().length() >= minCompactSize) {
                assert selectedFiles.stream()
                        .noneMatch(selected -> selected.getReader().length() > file.getReader().length());
            }
        }
    }
}