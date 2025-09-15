package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ViewFileSystem.RenameStrategy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;

public class ViewFileSystemRenameStrategyTest {

    private FileSystem fs;

    @Before
    public void setUp() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        // 1. Use the HDFS 2.8.5 API to obtain configuration values instead of hard-coding.
        String expectedStrategyStr = conf.get(
            Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
            RenameStrategy.SAME_MOUNTPOINT.toString());
        RenameStrategy expectedStrategy = RenameStrategy.valueOf(expectedStrategyStr);

        // 2. Prepare the test conditions: set up a minimal mount table so ViewFileSystem can initialize.
        conf.set("fs.viewfs.mounttable.cluster1.link./user", "file:///tmp/viewfs/user");

        URI uri = new URI("viewfs", "cluster1", "/", null, null);
        fs = FileSystem.get(uri, conf);

        // Ensure the source directory exists
        Path srcDir = new Path("/user/src");
        if (!fs.exists(srcDir)) {
            fs.mkdirs(srcDir);
        }
    }

    @Test
    public void explicitSameMountpointStrategyAccepted() throws IOException {
        // 3. Test code.
        boolean result = fs.rename(new Path("/user/src"), new Path("/user/dst"));

        // 4. Code after testing.
        assertTrue(result);
    }

    @After
    public void tearDown() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }
}