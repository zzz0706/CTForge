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
    public void sameMountpointBlocksCrossMountRename() throws Exception {
        // 1. Use Configuration without explicit set to rely on default/resource
        Configuration conf = new Configuration();
        conf.set("fs.viewfs.mounttable.cluster.link./mnt1", "hdfs://nn1:8020");
        conf.set("fs.viewfs.mounttable.cluster.link./mnt2", "hdfs://nn2:8020");

        // 2. Compute expected default value dynamically
        String expectedStrategy = conf.get(
                Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
                ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT.toString());

        // 3. Prepare mocks/stubs
        URI uri = new URI("viewfs://cluster/");
        Path srcPath = new Path("/mnt1/file");
        Path dstPath = new Path("/mnt2/file");

        FileSystem mockFs1 = mock(FileSystem.class);
        FileSystem mockFs2 = mock(FileSystem.class);

        when(mockFs1.getUri()).thenReturn(new URI("hdfs://nn1:8020"));
        when(mockFs2.getUri()).thenReturn(new URI("hdfs://nn2:8020"));

        PowerMockito.mockStatic(FileSystem.class);
        PowerMockito.when(FileSystem.get(any(URI.class), any(Configuration.class)))
                    .thenReturn(mockFs1, mockFs2);

        // 4. Create ViewFileSystem instance
        ViewFileSystem vfs = new ViewFileSystem();
        vfs.initialize(uri, conf);

        // 5. Invoke rename and assert
        try {
            vfs.rename(srcPath, dstPath);
            fail("Expected IOException for cross-mount rename");
        } catch (IOException e) {
            assertEquals("Renames across Mount points not supported", e.getMessage());
        }
    }
}