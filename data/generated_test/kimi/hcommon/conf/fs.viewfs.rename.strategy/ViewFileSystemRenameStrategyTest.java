package org.apache.hadoop.fs.viewfs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class})
public class ViewFileSystemRenameStrategyTest {

    @BeforeClass
    public static void setUpClass() {
        // Ensure a user name is available to avoid LoginException
        System.setProperty("user.name", "testuser");
        // In Hadoop 2.8.5, HADOOP_USER_NAME is also checked by UserGroupInformation
        System.setProperty("HADOOP_USER_NAME", "testuser");
    }

    @Test
    public void testAllRenameStrategies() throws Exception {
        // 1. Configuration with explicit values for each strategy
        Configuration conf = new Configuration();
        conf.set("fs.viewfs.mounttable.cluster.link./mnt1", "hdfs://nn1:8020");
        conf.set("fs.viewfs.mounttable.cluster.link./mnt2", "hdfs://nn2:8020");
        conf.set("fs.viewfs.mounttable.cluster.link./mnt3", "hdfs://nn1:8020"); // same authority as /mnt1
        conf.set("fs.viewfs.mounttable.cluster.link./mnt4", "hdfs://nn3:8020");

        // 2. Prepare mocks/stubs
        URI uri = new URI("viewfs://cluster/");
        Path srcPath = new Path("/mnt1/file");
        Path dstPath = new Path("/mnt2/file");
        Path sameAuthorityPath = new Path("/mnt3/file");
        Path diffAuthorityPath = new Path("/mnt4/file");

        FileSystem mockFs1 = mock(FileSystem.class);
        FileSystem mockFs2 = mock(FileSystem.class);
        FileSystem mockFs3 = mock(FileSystem.class);
        FileSystem mockFs4 = mock(FileSystem.class);

        when(mockFs1.getUri()).thenReturn(new URI("hdfs://nn1:8020"));
        when(mockFs2.getUri()).thenReturn(new URI("hdfs://nn2:8020"));
        when(mockFs3.getUri()).thenReturn(new URI("hdfs://nn1:8020"));
        when(mockFs4.getUri()).thenReturn(new URI("hdfs://nn3:8020"));

        PowerMockito.mockStatic(FileSystem.class);
        PowerMockito.when(FileSystem.get(any(URI.class), any(Configuration.class)))
                    .thenReturn(mockFs1, mockFs2, mockFs3, mockFs4);

        // 3. Test SAME_MOUNTPOINT (default)
        testStrategy(conf, uri, srcPath, dstPath, ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT, true);

        // 4. Test SAME_TARGET_URI_ACROSS_MOUNTPOINT
        testStrategy(conf, uri, srcPath, dstPath, ViewFileSystem.RenameStrategy.SAME_TARGET_URI_ACROSS_MOUNTPOINT, true);
        testStrategy(conf, uri, srcPath, srcPath, ViewFileSystem.RenameStrategy.SAME_TARGET_URI_ACROSS_MOUNTPOINT, false);

        // 5. Test SAME_FILESYSTEM_ACROSS_MOUNTPOINT
        testStrategy(conf, uri, srcPath, sameAuthorityPath, ViewFileSystem.RenameStrategy.SAME_FILESYSTEM_ACROSS_MOUNTPOINT, false);
        testStrategy(conf, uri, srcPath, diffAuthorityPath, ViewFileSystem.RenameStrategy.SAME_FILESYSTEM_ACROSS_MOUNTPOINT, true);

        // 6. Test invalid strategy value
        conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, "INVALID_STRATEGY");
        ViewFileSystem vfs = new ViewFileSystem();
        try {
            vfs.initialize(uri, conf);
            fail("Expected IllegalArgumentException for invalid strategy");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("No enum constant"));
        }
    }

    private void testStrategy(Configuration baseConf, URI uri, Path src, Path dst,
                             ViewFileSystem.RenameStrategy strategy,
                             boolean expectException) throws Exception {
        Configuration conf = new Configuration(baseConf);
        conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, strategy.toString());

        ViewFileSystem vfs = new ViewFileSystem();
        vfs.initialize(uri, conf);

        try {
            vfs.rename(src, dst);
            if (expectException) {
                fail("Expected IOException for strategy " + strategy);
            }
        } catch (IOException e) {
            if (!expectException) {
                fail("Unexpected IOException for strategy " + strategy + ": " + e.getMessage());
            }
            assertEquals("Renames across Mount points not supported", e.getMessage());
        }
    }
}