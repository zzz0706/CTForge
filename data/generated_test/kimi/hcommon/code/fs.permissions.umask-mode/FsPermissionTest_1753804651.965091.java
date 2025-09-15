package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FsPermissionTest {

    @Test
    public void testGetUMask_ExplicitOctalValue() {
        // 1. Instantiate Configuration inside the test method
        Configuration conf = new Configuration();

        // 2. Dynamically read the expected value from configuration
        //    (we still call conf.set here because the scenario explicitly asks to set "007")
        conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "007");
        long expectedUMask = 007;

        // 3. Invoke the method under test
        FsPermission actualPerm = FsPermission.getUMask(conf);
        long actualUMask = actualPerm.toShort();

        // 4. Assert the result
        assertEquals(expectedUMask, actualUMask);
    }
}