package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import java.util.Collections;

@Category({RegionServerTests.class, SmallTests.class})
public class TestFSHLogConstructor {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestFSHLogConstructor.class);

    private Configuration configuration;
    private FileSystem fileSystem;

    @Before
    public void setup() throws Exception {
        // Initialize configuration and filesystem
        configuration = new Configuration();
        
        // Ensure the tmp directory is properly set up and writable
        Path tempDir = new Path(System.getProperty("java.io.tmpdir"), "hbase-test");
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("hbase.rootdir", tempDir.toString());

        fileSystem = FileSystem.get(configuration);

        // Ensure the test directories exist before running the test
        Path rootDir = new Path(tempDir, "mock/root");
        fileSystem.mkdirs(new Path(rootDir, "dir"));
        fileSystem.mkdirs(new Path(rootDir, "log/dir"));
        fileSystem.mkdirs(new Path(rootDir, "archive/dir"));
    }

    @Test
    // Test code
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_FSHLog_Constructor_WAL_TolerateErrors() throws Exception {
        // 1. Obtain configuration values using HBase 2.2.2 API
        configuration.setInt("hbase.regionserver.logroll.errors.tolerated", 2);
        int closeErrorsTolerated = configuration.getInt(
                "hbase.regionserver.logroll.errors.tolerated", 2);

        // 2. Prepare test conditions
        Path rootDir = new Path(System.getProperty("java.io.tmpdir"), "hbase-test/mock/root/dir");
        String logDir = rootDir.toString() + "/log/dir"; // Adjusted to ensure consistent directory structure
        String archiveDir = rootDir.toString() + "/archive/dir"; // Adjusted to ensure consistent directory structure
        boolean failIfWalExists = false;
        String prefix = "hostname_port";
        String suffix = ".wal";

        // Create required dependencies (listeners can be empty for this test case)
        FSHLog fshLog = new FSHLog(
                fileSystem,
                rootDir,
                logDir,
                archiveDir,
                configuration,
                Collections.emptyList(),
                failIfWalExists,
                prefix,
                suffix
        );

        // 3. Test the closeErrorsTolerated value
        // Using reflection to access the private field `closeErrorsTolerated` in FSHLog
        java.lang.reflect.Field field = FSHLog.class.getDeclaredField("closeErrorsTolerated");
        field.setAccessible(true);
        int actualCloseErrorsTolerated = field.getInt(fshLog);

        // 4. Assert the value
        assert closeErrorsTolerated == actualCloseErrorsTolerated;
    }
}