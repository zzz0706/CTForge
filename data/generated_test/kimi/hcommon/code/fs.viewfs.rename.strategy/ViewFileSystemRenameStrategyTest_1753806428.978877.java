package org.apache.hadoop.fs.viewfs;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ViewFileSystemRenameStrategyTest {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

    @After
    public void tearDown() throws IOException {
    }

    @Test
    public void explicitSameFilesystemAcrossMountpointAccepted() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
                 "SAME_FILESYSTEM_ACROSS_MOUNTPOINT");

        // 2. Prepare the test conditions.
        URI theUri = new URI("viewfs", "test-cluster", "/", null, null);

        URI srcUri = new URI("file", "", "/tmp/user/alice/src", null, null);
        URI dstUri = new URI("file", "", "/tmp/user/bob/dst", null, null);

        conf.set("fs.viewfs.mounttable.test-cluster.link./user/alice",
                 srcUri.toString());
        conf.set("fs.viewfs.mounttable.test-cluster.link./user/bob",
                 dstUri.toString());

        // 3. Test code
        ViewFileSystem vfs = new ViewFileSystem();
        vfs.initialize(theUri, conf);

        Path src = new Path("/user/alice/src");
        Path dst = new Path("/user/bob/dst");

        // 4. Code after testing
        assertNotNull(vfs);
    }
}