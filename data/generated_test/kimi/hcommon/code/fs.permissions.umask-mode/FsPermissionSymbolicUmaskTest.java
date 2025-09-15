package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FsPermissionSymbolicUmaskTest {

    @Test
    public void testGetUMask_SymbolicValue() {
        // 1. Create a fresh Configuration instance
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions – set symbolic umask
        conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "u=rwx,g=rwx,o=");

        // 3. Invoke the method under test
        FsPermission actualUmask = FsPermission.getUMask(conf);

        // 4. Compute the expected value dynamically
        short expectedUmask = 007; // symbolic "u=rwx,g=rwx,o=" translates to 007 (octal)

        // 5. Assert the result
        assertEquals(expectedUmask, actualUmask.toShort());
    }
}