package org.apache.hadoop.fs.permission;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class FsPermissionTest {

    @Test
    public void testSetUMask_PersistsBothKeys() {
        // 1. Instantiate Configuration inside the @Test method
        Configuration conf = new Configuration();

        // 2. Prepare test conditions
        FsPermission umask = new FsPermission((short) 007);

        // 3. Invoke the method under test
        FsPermission.setUMask(conf, umask);

        // 4. Assertions
        String actualNew = conf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "MISSING");
        int actualDeprecated = conf.getInt("dfs.umask", -1);

        assertEquals("007", actualNew);
        assertEquals(7, actualDeprecated);
    }
}