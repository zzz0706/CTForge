package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
public class TestExploringCompactionPolicy {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestExploringCompactionPolicy.class);

    @Test // Test annotation
    // test code
    public void testApplyCompactionPolicy_ExcludingLargeFiles() throws Exception {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        HBaseTestingUtility testingUtility = new HBaseTestingUtility();
        Configuration configuration = testingUtility.getConfiguration();

        long maxCompactSize = 1024L * 1024 * 100; // 100MB
        configuration.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_SIZE_KEY, maxCompactSize);

        // 2. Prepare the test conditions.
        // Create mock HStoreFile objects with different sizes
        List<HStoreFile> candidates = new ArrayList<>();
        candidates.add(createMockHStoreFile(maxCompactSize - 100)); // Below threshold
        candidates.add(createMockHStoreFile(maxCompactSize + 100)); // Above threshold

        // Mock StoreConfigInformation
        StoreConfigInformation mockStoreConfigInfo = mock(StoreConfigInformation.class);

        // 3. Test code.
        // Instantiate ExploringCompactionPolicy and CompactionConfiguration
        CompactionConfiguration compactionConfiguration = new CompactionConfiguration(configuration, mockStoreConfigInfo);
        ExploringCompactionPolicy exploringPolicy = new ExploringCompactionPolicy(configuration, mockStoreConfigInfo);

        // Apply compaction policy with provided conditions
        List<HStoreFile> result = exploringPolicy.applyCompactionPolicy(candidates, false, false, 1, candidates.size());

        // 4. Code after testing.
        // Assert that files exceeding maxCompactSize are excluded
        for (HStoreFile file : result) {
            Assert.assertFalse("File exceeding maxCompactSize should not be included.",
                    file.getReader().length() > maxCompactSize);
        }
    }

    /**
     * Helper method to create a mock HStoreFile with a given size.
     *
     * @param size the size of the file
     * @return a mocked HStoreFile instance
     * @throws Exception in case of errors
     */
    private HStoreFile createMockHStoreFile(long size) throws Exception {
        Path mockPath = mock(Path.class);
        when(mockPath.toString()).thenReturn("mock-file-path");

        StoreFileReader mockReader = mock(StoreFileReader.class);
        when(mockReader.length()).thenReturn(size);

        HStoreFile mockHStoreFile = mock(HStoreFile.class);
        when(mockHStoreFile.getReader()).thenReturn(mockReader);

        return mockHStoreFile;
    }
}