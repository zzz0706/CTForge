package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.*;

public class ViewFileSystemRenameStrategyTest {

    private FileSystem fs;
    private Configuration conf;

    @Before
    public void setUp() throws IOException, URISyntaxException {
        conf = new Configuration();
        // 1. Use the Hadoop 2.8.5 API to obtain configuration values
        String expectedStrategyStr = conf.get(
            Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
            ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT.toString());
        ViewFileSystem.RenameStrategy expectedStrategy = ViewFileSystem.RenameStrategy.valueOf(expectedStrategyStr);

        // 2. Prepare test conditions: minimal mount table
        conf.set("fs.viewfs.mounttable.cluster1.link./user", "file:///tmp/viewfs/user");
        conf.set("fs.viewfs.mounttable.cluster1.link./data", "file:///tmp/viewfs/data");
        conf.set("fs.viewfs.mounttable.cluster1.link./common", "file:///tmp/viewfs/common");

        URI uri = new URI("viewfs", "cluster1", "/", null, null);
        fs = FileSystem.get(uri, conf);

        // Ensure source directory exists
        Path srcDir = new Path("/user/src");
        if (!fs.exists(srcDir)) {
            fs.mkdirs(srcDir);
        }
    }

    @Test
    public void testDefaultConfiguration() throws IOException, URISyntaxException {
        // 3. Test code - verify default strategy is used
        String actualStrategy = conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
            ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT.toString());
        assertEquals(ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT.toString(), actualStrategy);
        
        // 4. Verify rename works within same mount point
        boolean result = fs.rename(new Path("/user/src"), new Path("/user/dst"));
        assertTrue(result);
    }

    @Test
    public void testExplicitSameMountpointStrategy() throws IOException, URISyntaxException {
        // 1. Set configuration explicitly
        conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT.toString());
        
        // 2. Re-initialize with new config
        URI uri = new URI("viewfs", "cluster1", "/", null, null);
        FileSystem fs2 = FileSystem.get(uri, conf);
        
        // 3. Test code - verify rename within same mount point
        Path src = new Path("/user/src");
        Path dst = new Path("/user/dst2");
        boolean result = fs2.rename(src, dst);
        assertTrue(result);
        
        // 4. Cleanup
        fs2.close();
    }

    @Test
    public void testSameTargetUriStrategyConfiguration() throws IOException, URISyntaxException {
        // 1. Set configuration for same target URI strategy
        conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, 
            ViewFileSystem.RenameStrategy.SAME_TARGET_URI_ACROSS_MOUNTPOINT.toString());
        
        // 2. Prepare mount table with same target
        conf.set("fs.viewfs.mounttable.cluster2.link./user", "file:///tmp/viewfs/common");
        conf.set("fs.viewfs.mounttable.cluster2.link./data", "file:///tmp/viewfs/common");
        
        URI uri = new URI("viewfs", "cluster2", "/", null, null);
        FileSystem fs2 = FileSystem.get(uri, conf);
        
        // 3. Test code - verify cross-mountpoint rename allowed when target URIs match
        Path src = new Path("/user/src");
        Path dst = new Path("/data/src");
        if (!fs2.exists(src)) {
            fs2.mkdirs(src);
        }
        boolean result = fs2.rename(src, dst);
        assertTrue(result);
        
        // 4. Cleanup
        fs2.close();
    }

    @Test
    public void testSameFilesystemStrategyConfiguration() throws IOException, URISyntaxException {
        // 1. Set configuration for same filesystem strategy
        conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, 
            ViewFileSystem.RenameStrategy.SAME_FILESYSTEM_ACROSS_MOUNTPOINT.toString());
        
        // 2. Prepare mount table with same filesystem
        conf.set("fs.viewfs.mounttable.cluster3.link./user", "file:///tmp/viewfs/user");
        conf.set("fs.viewfs.mounttable.cluster3.link./data", "file:///tmp/viewfs/user");
        
        URI uri = new URI("viewfs", "cluster3", "/", null, null);
        FileSystem fs3 = FileSystem.get(uri, conf);
        
        // 3. Test code - verify cross-mountpoint rename allowed when filesystem same
        Path src = new Path("/user/src");
        Path dst = new Path("/data/src");
        if (!fs3.exists(src)) {
            fs3.mkdirs(src);
        }
        boolean result = fs3.rename(src, dst);
        assertTrue(result);
        
        // 4. Cleanup
        fs3.close();
    }

    @Test
    public void testInvalidStrategyConfiguration() throws IOException, URISyntaxException {
        // 1. Set invalid configuration
        conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, "INVALID_STRATEGY");
        
        // 2. Verify initialization fails with IllegalArgumentException
        URI uri = new URI("viewfs", "cluster1", "/", null, null);
        try {
            FileSystem.get(uri, conf);
            fail("Expected IllegalArgumentException for invalid strategy");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unexpected rename strategy"));
        }
    }

    @After
    public void tearDown() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }
}