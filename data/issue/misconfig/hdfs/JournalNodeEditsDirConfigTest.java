package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;
//hdfs-3795
public class JournalNodeEditsDirConfigTest {

    @Test
    public void testJournalNodeEditsDirFormat() {
        Configuration conf = new Configuration();
        String editsDir = conf.get("dfs.journalnode.edits.dir", null);

        // Allow the configuration to be unset or empty
        if (editsDir == null || editsDir.trim().isEmpty()) {
            return; // Valid if unset or empty
        }

        String trimmed = editsDir.trim();

        // The value must be an absolute path, not a URI
        File dir = new File(trimmed);
        assertTrue(
            "dfs.journalnode.edits.dir must be an absolute path: " + trimmed,
            dir.isAbsolute()
        );

        // Should not look like a URI (e.g. file://)
        assertFalse(
            "dfs.journalnode.edits.dir must not be a URI: " + trimmed,
            trimmed.matches("^[a-zA-Z][a-zA-Z0-9+.-]*:.*")
        );
    }
}
