package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFsPermissionUmask {

    /**
     * Test to validate the configuration value for fs.permissions.umask-mode
     * - Ensure umask is either symbolic or octal
     */
    @Test
    public void testFsPermissionsUmaskValidValue() {
        Configuration conf = new Configuration();

        // Simulate loading the configuration value
        String umaskValue = conf.get("fs.permissions.umask-mode", "022");

        try {
            // Attempt to parse symbolic/octal value using FsPermission
            FsPermission.getUMask(conf);
            // If parsing is successful, configuration value is valid
            assertTrue(true);
        } catch (IllegalArgumentException e) {
            // If parsing fails, configuration value is invalid
            fail("Invalid configuration value for fs.permissions.umask-mode: " + umaskValue);
        }
    }

    /**
     * Test to ensure deprecated dfs.umask is handled correctly when present
     */
    @Test
    public void testDeprecatedUmaskConfiguration() {
        Configuration conf = new Configuration();

        // Simulate loading deprecated configuration
        conf.setInt("dfs.umask", 18); // Old decimal-based umask
        conf.set("fs.permissions.umask-mode", "022"); // New octal-based umask
        
        String newUmaskValue = conf.get("fs.permissions.umask-mode");
        int oldUmaskValue = conf.getInt("dfs.umask", Integer.MIN_VALUE);

        try {
            int parsedNewUmask = FsPermission.getUMask(conf).toShort();
            // If both configurations are present, ensure backward compatibility
            if (oldUmaskValue != Integer.MIN_VALUE && parsedNewUmask != oldUmaskValue) {
                assertEquals("Backward compatibility issue. Old and new umask values should match.", oldUmaskValue, parsedNewUmask);
            }
            // Else test passes
        } catch (IllegalArgumentException e) {
            fail("Invalid configuration handling for fs.permissions.umask-mode or dfs.umask.");
        }
    }

    /**
     * Test to verify failure on invalid configuration value for fs.permissions.umask-mode
     * - Ensure invalid values throw exceptions
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUmaskValue() {
        Configuration conf = new Configuration();

        // Simulate invalid configuration value
        conf.set("fs.permissions.umask-mode", "invalid_value");

        // Attempt to parse the invalid value, should throw IllegalArgumentException
        FsPermission.getUMask(conf).toShort();
    }

    /**
     * Test to ensure presence of the configuration fs.permissions.umask-mode
     */
    @Test
    public void testConfigurationPresence() {
        Configuration conf = new Configuration();

        // Simulate loading configuration
        String umaskValue = conf.get("fs.permissions.umask-mode", null);

        // Assert the configuration presence (could be default or explicitly set)
        assertNotNull("Configuration fs.permissions.umask-mode should be present.", umaskValue);
    }
}