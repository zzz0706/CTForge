package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFsPermissionsUmaskConfigValidation {

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
    }

    @After
    public void tearDown() {
        conf = null;
    }

    /**
     * Validates that the umask-mode configuration value is syntactically correct
     * and can be parsed by UmaskParser.
     * Acceptable formats:
     *   - Octal: "022", "0", "777"
     *   - Symbolic: "u=rwx,g=r-x,o=r-x", "u=rwx,g=rwx,o="
     */
    @Test
    public void testValidUmaskMode() {
        // Should not throw any exception for valid octal
        String[] validOctal = {"000", "022", "077"};
        for (String val : validOctal) {
            conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, val);
            FsPermission umask = FsPermission.getUMask(conf);
            assertNotNull("Valid octal umask should not return null: " + val, umask);
        }

        // Should not throw any exception for valid symbolic
        String[] validSymbolic = {"a-w", "g-w,o-w"};
        for (String val : validSymbolic) {
            conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, val);
            FsPermission umask = FsPermission.getUMask(conf);
            assertNotNull("Valid symbolic umask should not return null: " + val, umask);
        }
    }

    /**
     * Verifies that invalid umask-mode configuration values cause an
     * IllegalArgumentException to be thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUmaskModeOctal() {
        conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "999"); // invalid octal digit
        FsPermission.getUMask(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUmaskModeSymbolic() {
        conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "u=xyz"); // invalid symbolic
        FsPermission.getUMask(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUmaskModeMalformed() {
        conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "abc123"); // malformed
        FsPermission.getUMask(conf);
    }

    /**
     * Ensures that an empty umask-mode falls back to the default 022.
     */
    @Test
    public void testEmptyUmaskMode() {
        // Remove the key to ensure default is used
        conf.unset(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY);
        FsPermission umask = FsPermission.getUMask(conf);
        assertNotNull(umask);
        assertEquals("Empty umask should fall back to default 022", 022, umask.toShort() & 0777);
    }

    /**
     * Verifies that a missing umask-mode falls back to the default 022.
     */
    @Test
    public void testMissingUmaskMode() {
        // Do not set the key at all
        FsPermission umask = FsPermission.getUMask(conf);
        assertNotNull(umask);
        assertEquals("Missing umask should fall back to default 022", 022, umask.toShort() & 0777);
    }
}