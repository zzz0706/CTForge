package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class FsPermissionTest {

    @Test
    public void testGetUMask_InvalidNewKeyFallsBackToDeprecatedKey() {
        // 1. Instantiate Configuration inside the @Test method
        Configuration conf = new Configuration();

        // 2. Prepare test conditions
        conf.set("fs.permissions.umask-mode", "invalid");
        conf.setInt("dfs.umask", 027);

        // 3. Test code - invoke the method under test
        FsPermission actual = FsPermission.getUMask(conf);

        // 4. Code after testing - assertions
        assertEquals("Expected umask 027 (octal)", 027, actual.toShort());
    }
}