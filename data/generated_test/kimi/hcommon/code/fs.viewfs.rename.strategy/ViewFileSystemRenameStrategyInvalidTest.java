package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ViewFileSystemRenameStrategyInvalidTest {

    @Test
    public void invalidRenameStrategyThrowsIllegalArgumentException() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        // 1. Use the correct constant key for rename strategy
        conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, "INVALID_STRATEGY");

        // 2. Prepare minimal mount table so ViewFs can initialize
        URI uri = new URI("viewfs://test-cluster/");
        conf.set("fs.viewfs.mounttable.test-cluster.homedir", "/user");
        conf.set("fs.viewfs.mounttable.test-cluster.link./user", "file:///tmp");

        try {
            FileSystem fs = FileSystem.newInstance(uri, conf);
            fail("Expected IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            // 3. IllegalArgumentException is thrown directly in 2.8.5
            assertTrue("Exception should be IllegalArgumentException",
                       e instanceof IllegalArgumentException);
        } catch (RuntimeException e) {
            // In 2.8.5 the IllegalArgumentException is thrown directly, not wrapped
            if (e instanceof IllegalArgumentException) {
                assertTrue("Exception should be IllegalArgumentException",
                           e instanceof IllegalArgumentException);
            } else {
                fail("Unexpected exception type: " + e.getClass());
            }
        } finally {
            // 4. No extra teardown required – FileSystem is closed in try-with-resources or test ends
        }
    }
}