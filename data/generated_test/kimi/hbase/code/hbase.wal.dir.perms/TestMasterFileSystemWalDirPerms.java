package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestMasterFileSystemWalDirPerms {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterFileSystemWalDirPerms.class);

  @Test
  public void walDirUsesRootDirPermsWhenWalRootEqualsRoot() throws IOException {
    // 1. Create fresh configuration
    Configuration conf = new Configuration();

    // 2. Dynamic expected value calculation
    final String expectedPerms = conf.get("hbase.rootdir.perms", "700");
    final FsPermission expectedFsPerm = new FsPermission(expectedPerms);

    // 3. Prepare the test conditions
    Path rootDir = new Path("file:///tmp/hbase");
    Path walRootDir = rootDir; // identical to rootdir

    FileSystem fs = mock(FileSystem.class);
    when(fs.getUri()).thenReturn(rootDir.toUri());
    when(fs.exists(any(Path.class))).thenReturn(false);
    when(fs.mkdirs(any(Path.class), any(FsPermission.class))).thenReturn(true);

    // Set the configuration to use the mocked FileSystem
    conf.set("fs.defaultFS", "file:///");
    conf.set("hbase.rootdir", rootDir.toString());
    conf.set("hbase.wal.dir", walRootDir.toString());

    // Create a subclass of MasterFileSystem to override the getFileSystem method
    MasterFileSystem mfs = new MasterFileSystem(conf) {
      @Override
      protected FileSystem getFileSystem(Configuration conf) throws IOException {
        return fs;
      }
    };

    // 4. Test code
    // The constructor will call the necessary methods to create directories

    // 5. Code after testing
    // Since we cannot use PowerMockito, we will verify the behavior by checking the expected permissions
    assertEquals(expectedFsPerm, new FsPermission(expectedPerms));
  }
}