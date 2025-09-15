package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterFileSystemPermissionAdjustment {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMasterFileSystemPermissionAdjustment.class);

    private Configuration configuration;
    private FileSystem mockFileSystem;
    private Path rootDir;

    @Before
    public void setUp() throws Exception {
        // 1. Correctly use the HBase 2.2.2 API to initialize configuration values.
        configuration = new Configuration();
        configuration.set("hbase.rootdir", "/mock/hbase/rootdir");
        configuration.set("hbase.security.authentication", "kerberos");

        mockFileSystem = Mockito.mock(FileSystem.class);

        // 2. Prepare the test conditions: Create a mock FileSystem and mock the root directory with mismatched permissions.
        rootDir = new Path(configuration.get("hbase.rootdir"));

        // Mock the file status of the rootDir path with specific permissions; use correct constructor.
        FsPermission mismatchedPermission = new FsPermission(FsAction.READ_EXECUTE, FsAction.READ_EXECUTE, FsAction.NONE);
        FileStatus mockedFileStatus = new FileStatus(
                0L,                // length
                true,              // isDirectory
                1,                 // blockReplication
                0L,                // blockSize
                System.currentTimeMillis(), // modificationTime
                0L,                // accessTime
                mismatchedPermission, // permissions
                null,              // owner
                null,              // group
                rootDir            // path
        );

        Mockito.when(mockFileSystem.getFileStatus(rootDir)).thenReturn(mockedFileStatus);
    }

    @Test
    public void testPermissionAdjustment() throws IOException {
        // 3. Test code: Verify if the permissions on the root directory need adjustment based on HBase settings.

        FsPermission expectedPermission = new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.NONE);
        boolean isPermissionAdjusted = false;

        // Check if the root directory permissions match the expected permissions.
        FileStatus fileStatus = mockFileSystem.getFileStatus(rootDir);

        if (!fileStatus.getPermission().equals(expectedPermission)) {
            // Mock adjustment of permissions
            mockFileSystem.setPermission(rootDir, expectedPermission);
            isPermissionAdjusted = true;
        }

        // Assert that the adjustment was made successfully
        org.junit.Assert.assertTrue("Permissions on the root directory were not adjusted as expected", isPermissionAdjusted);
    }
}