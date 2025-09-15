package org.apache.hadoop.fs;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.FsCommand;
import org.junit.Test;

public class FileSystemConfigTest {

    @Test
    public void testFileSystemGetUsesDefaultUriFromConfiguration() throws Exception {
        // 1. Use the hadoop-common 2.8.5 API correctly to obtain configuration values.
        Configuration conf = new Configuration(false);

        // 2. Prepare the test conditions – exercise different code paths.
        // 2a. Test default fallback
        String defaultFs = conf.get(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_DEFAULT);
        URI expectedUri = URI.create(defaultFs);

        // 2b. Test explicit value
        conf.set(FS_DEFAULT_NAME_KEY, "file:///");
        URI fileUri = FileSystem.getDefaultUri(conf);
        assertEquals("file", fileUri.getScheme());
        assertNull(fileUri.getAuthority());

        // 2c. Test deprecated key propagation
        conf = new Configuration(false);
        conf.set("fs.default.name", "file:///");
        URI viewFsUri = FileSystem.getDefaultUri(conf);
        assertEquals("file", viewFsUri.getScheme());

        // 2d. Test scheme-only URI triggers authority stitching
        conf = new Configuration(false);
        conf.set(FS_DEFAULT_NAME_KEY, "file:///");
        URI partial = URI.create("file:///tmp");
        FileSystem fs = FileSystem.get(partial, conf);
        assertEquals("file", fs.getUri().getScheme());

        // 2e. Test FileContext path through empty scheme
        conf = new Configuration(false);
        conf.set(FS_DEFAULT_NAME_KEY, "file:///");
        FileContext fc = FileContext.getFileContext(conf);
        assertNotNull(fc);

        // 2f. Test FsCommand warning on default value
        conf = new Configuration(false);
        conf.set(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_DEFAULT);
        TestFsCommand cmd = new TestFsCommand(conf);
        LinkedList<String> args = new LinkedList<>();
        cmd.processRawArgumentsPublic(args);
        assertTrue(cmd.warningEmitted);

        // 2g. Test null scheme rejection
        conf = new Configuration(false);
        conf.set(FS_DEFAULT_NAME_KEY, "/no/scheme/path");
        try {
            FileContext.getFileContext(conf);
            fail("Expected UnsupportedFileSystemException");
        } catch (UnsupportedFileSystemException expected) {
            assertTrue(expected.getMessage().contains("carries no scheme"));
        }

        // 2h. Test initialize with authority fallback
        conf = new Configuration(false);
        conf.set(FS_DEFAULT_NAME_KEY, "file:///");
        FileSystem raw = FileSystem.newInstance(conf);
        raw.initialize(new URI("file", null, "/tmp", null, null), conf);
        assertEquals("file", raw.getUri().getScheme());
    }

    // Helper FsCommand subclass to capture warnings
    private static class TestFsCommand extends FsCommand {
        boolean warningEmitted = false;

        TestFsCommand(Configuration conf) {
            setConf(conf);
        }

        // Expose the protected method for test invocation
        public void processRawArgumentsPublic(LinkedList<String> args) throws IOException {
            processRawArguments(args);
        }

        @Override
        protected void processArguments(LinkedList<org.apache.hadoop.fs.shell.PathData> args) { /* no-op */ }
    }
}