package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FsPermissionTest {

    @Test
    public void testGetUMask_DeprecatedKeyTakesPrecedence() {
        // 1. Create Configuration
        Configuration conf = new Configuration();

        // 2. Prepare the test conditions
        conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "007");
        conf.setInt("dfs.umask", 027);

        // 3. Test code
        FsPermission actual = FsPermission.getUMask(conf);

        // 4. Code after testing
        assertEquals(027, actual.toShort());
    }
}