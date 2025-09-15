package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestFsPermission {     

    @Test
    public void testBackwardCompatibilityWithDeprecatedKey() {
        // Step 1: Create a Hadoop Configuration object
        Configuration configuration = new Configuration();

        // Step 2: Ensure the deprecated `dfs.umask` key exists in the configuration with a valid value
        String testUmaskValue = "022"; // Valid umask value in octal
        configuration.set("dfs.umask", testUmaskValue);

        // Step 3: Retrieve the value of the deprecated key `dfs.umask`
        String deprecatedValue = configuration.get("dfs.umask");

        // Step 4: Sanity check to ensure the value retrieved from the deprecated key is not null
        assertNotNull("The deprecated key dfs.umask should have a valid value.", deprecatedValue);

        // Step 5: Call `setUMask` to manually set the umask from the deprecated key
        FsPermission.setUMask(configuration, new FsPermission((short) Integer.parseInt(testUmaskValue, 8)));

        // Step 6: Call `getUMask` to read the umask from the Configuration object
        FsPermission umaskPermission = FsPermission.getUMask(configuration);

        // Step 7: Validate the resulting FsPermission object
        assertNotNull("FsPermission object should not be null when processed correctly.", umaskPermission);

        // Step 8: Verify the permission value matches the expected value derived from the `dfs.umask` key
        short expectedPermission = (short) Integer.parseInt(testUmaskValue, 8);
        assertEquals("FsPermission umask should match the value from the deprecated key.", expectedPermission, umaskPermission.toShort());
    }
}