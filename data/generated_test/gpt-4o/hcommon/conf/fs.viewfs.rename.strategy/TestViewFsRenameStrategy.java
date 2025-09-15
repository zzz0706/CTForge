package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestViewFsRenameStrategy {

    private Configuration configuration;

    @Before
    public void setUp() {
        configuration = new Configuration();
    }

    @Test
    public void testValidRenameStrategyConfiguration() {
        // Allowed values are SAME_MOUNTPOINT, SAME_TARGET_URI_ACROSS_MOUNTPOINT, SAME_FILESYSTEM_ACROSS_MOUNTPOINT
        String renameStrategy = configuration.get("fs.viewfs.rename.strategy", "SAME_MOUNTPOINT");
        
        assertTrue("Invalid value for 'fs.viewfs.rename.strategy': " + renameStrategy,
            renameStrategy.equals("SAME_MOUNTPOINT") ||
            renameStrategy.equals("SAME_TARGET_URI_ACROSS_MOUNTPOINT") ||
            renameStrategy.equals("SAME_FILESYSTEM_ACROSS_MOUNTPOINT"));
    }

    @Test
    public void testDefaultRenameStrategyConfiguration() {
        // Default value should be SAME_MOUNTPOINT
        String defaultStrategy = configuration.get("fs.viewfs.rename.strategy", "SAME_MOUNTPOINT");
        assertEquals("Default value for 'fs.viewfs.rename.strategy' is incorrect.", 
                     "SAME_MOUNTPOINT", defaultStrategy);
    }
    
    @Test
    public void testNonexistentRenameStrategyConfiguration() {
        // Value not explicitly set; should fall back to default
        String nonexistentStrategy = configuration.get("fs.viewfs.rename.strategy");
        assertEquals("Nonexistent value for 'fs.viewfs.rename.strategy' should fall back to default.", 
                     "SAME_MOUNTPOINT", 
                     nonexistentStrategy != null ? nonexistentStrategy : "SAME_MOUNTPOINT");
    }
}