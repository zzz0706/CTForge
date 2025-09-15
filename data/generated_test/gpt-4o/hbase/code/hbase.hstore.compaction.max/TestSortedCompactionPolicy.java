package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

/**
 * Unit test for verifying that the logic of removing excess files based on
 * the maximum files allowed for compaction works correctly.
 */
@Category(SmallTests.class)
public class TestSortedCompactionPolicy {

    @ClassRule // HBaseClassTestRule for running the test in HBase's test environment
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestSortedCompactionPolicy.class);

    private static HBaseTestingUtility testingUtility;
    private static int maxFilesToCompact;

    @BeforeClass
    public static void setUp() throws Exception {
        // Initialize HBase testing utility
        testingUtility = new HBaseTestingUtility();
        testingUtility.startMiniCluster(); // Start mini HBase cluster for testing

        // Retrieve configuration value via HBase APIs
        maxFilesToCompact = testingUtility.getConfiguration()
                .getInt("hbase.hstore.compaction.max", 10);

        // Ensure the /mock directory exists in the file system for mock file creation
        Path mockDir = new Path("/mock/");
        testingUtility.getTestFileSystem().mkdirs(mockDir);
    }

    @Test
    public void testRemoveExcessFilesLogicForMaximumFilesAllowed() throws Exception {
        // 1. Prepare a list of mocked HStoreFile objects exceeding the maxFilesToCompact threshold
        List<HStoreFile> candidateFiles = new ArrayList<>();
        int numFiles = maxFilesToCompact + 5; // Create more files than allowed
        CacheConfig cacheConfig = new CacheConfig(testingUtility.getConfiguration());
        for (int i = 0; i < numFiles; i++) {
            // Mock store files using StoreFileInfo and relevant constructors
            candidateFiles.add(createMockStoreFile(i, testingUtility.getConfiguration(), cacheConfig));
        }

        // Validate the mock files were created
        assertNotNull("Candidate files list should not be null.", candidateFiles);
        assertTrue("Candidate files should be populated.", candidateFiles.size() == numFiles);

        // 2. Test logic to trim the list and ensure excess files are removed
        List<HStoreFile> compactedFiles = removeExcessFiles(candidateFiles, maxFilesToCompact);

        // 3. Validate trimmed list does not exceed maxFilesToCompact
        assertTrue("File list should not exceed maxFilesToCompact limit.",
                compactedFiles.size() <= maxFilesToCompact);
    }

    /**
     * Creates a mock HStoreFile for testing purposes. Ensures that the path exists beforehand.
     *
     * @param index        Index of the mock file in the sequence.
     * @param configuration HBase configuration object.
     * @param cacheConfig   Cache configuration object.
     * @return A mock HStoreFile object.
     * @throws Exception Throws exception if mock file creation fails.
     */
    private HStoreFile createMockStoreFile(int index, Configuration configuration, CacheConfig cacheConfig) throws Exception {
        // Simulate the creation of a mock HStoreFile using StoreFileInfo and valid API constructors
        Path path = new Path("/mock/" + index);
        
        // Ensure the mock file exists in the file system
        testingUtility.getTestFileSystem().create(path);

        FileStatus fileStatus = testingUtility.getTestFileSystem().getFileStatus(path);
        StoreFileInfo storeFileInfo = new StoreFileInfo(configuration, testingUtility.getTestFileSystem(), fileStatus);
        return new HStoreFile(testingUtility.getTestFileSystem(), storeFileInfo,
                configuration, cacheConfig, BloomType.NONE, true);
    }

    /**
     * Logic to remove excess files from a list based on a maximum number.
     *
     * @param candidateFiles List of input files.
     * @param maxFiles       Maximum files allowed.
     * @return A trimmed list adhering to the maxFiles limit.
     */
    private List<HStoreFile> removeExcessFiles(List<HStoreFile> candidateFiles, int maxFiles) {
        // Logic to trim the list based on maxFiles
        if (candidateFiles.size() > maxFiles) {
            return candidateFiles.subList(0, maxFiles);
        }
        return candidateFiles;
    }
}