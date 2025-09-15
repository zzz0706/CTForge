package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class FsPermissionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testGetUMask_InvalidNewKeyNoDeprecatedKeyThrowsException() {
        // 1. Instantiate Configuration
        Configuration conf = new Configuration();

        // 2. Set invalid value for new key
        conf.set("fs.permissions.umask-mode", "invalid");

        // 3. Attempt to get umask - should throw IllegalArgumentException
        FsPermission.getUMask(conf);
    }
}