package org.apache.hadoop.fs.permission;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class FsPermissionTest {

    @Test
    public void testGetUMask_DefaultOctalValue() {
        // 1. Create a fresh Configuration instance without any umask-related keys set
        Configuration conf = new Configuration();

        // 2. Compute the expected value dynamically from the configuration
        String defaultUmaskStr = conf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "022");
        int expectedUmask = new UmaskParser(defaultUmaskStr).getUMask();

        // 3. Invoke the method under test
        FsPermission actualPermission = FsPermission.getUMask(conf);

        // 4. Assert the result
        assertEquals("Expected default umask 022 (octal)", expectedUmask, actualPermission.toShort());
    }
}