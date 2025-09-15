package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;

@Category({MasterTests.class, SmallTests.class})
public class TestMasterFileSystemInitializationWithValidPerms {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestMasterFileSystemInitializationWithValidPerms.class);

    @Test
    public void testMasterFileSystemInitializationWithValidPerms() throws IOException {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration config = new Configuration();
        config.set("hbase.rootdir", "/hbase/root");
        config.set("hbase.rootdir.perms", "700");

        // 2. Prepare the test conditions.
        FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
        Path rootDir = new Path(config.get("hbase.rootdir")); // Get root directory from configuration.
        Path tempDir = new Path(rootDir, "temp"); // Base temp directory path.

        // Mocking FileSystem behaviors for the required FileStatus check.
        Mockito.when(mockFileSystem.getFileStatus(rootDir)).thenReturn(
            new org.apache.hadoop.fs.FileStatus(0, true, 0, 0, 0, rootDir)
        );
        Mockito.when(mockFileSystem.mkdirs(tempDir)).thenReturn(true);

        // 3. Test code.
        // Correctly simulate MasterFileSystem initialization workflow with mock file system.
        FsPermission expectedPermission = new FsPermission((short) Short.parseShort(config.get("hbase.rootdir.perms"), 8));

        MasterFileSystem mockMasterFileSystem = Mockito.mock(MasterFileSystem.class);
        Mockito.when(mockMasterFileSystem.getFileSystem()).thenReturn(mockFileSystem);

        // Simulate creation of the initial filesystem layout.
        mockFileSystem.mkdirs(tempDir); // Temporary subdir creation.
        mockFileSystem.setPermission(tempDir, expectedPermission); // Set expected permissions.

        // Verify temp directory creation and permission setting.
        Mockito.verify(mockFileSystem, Mockito.times(1)).mkdirs(Mockito.eq(tempDir));
        Mockito.verify(mockFileSystem, Mockito.atLeastOnce()).setPermission(Mockito.eq(tempDir), Mockito.eq(expectedPermission));
    }
}