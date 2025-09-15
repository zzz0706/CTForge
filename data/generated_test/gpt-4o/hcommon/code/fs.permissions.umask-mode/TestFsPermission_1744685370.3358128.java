package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFsPermission {


    @Test
    public void testInvalidUmaskValueHandlingUnderWorkload() {
        Configuration configuration = new Configuration();
        
        configuration.set("fs.permissions.umask-mode", "999"); 

        try {

            FsPermission.getUMask(configuration);

            fail("Expected IllegalArgumentException not thrown for invalid umask value");
        } catch (IllegalArgumentException e) {
            assertTrue("Exception message does not indicate invalid umask value",
                       e.getMessage() != null 
                       && (e.getMessage().contains("Invalid") || e.getMessage().contains("umask"))); 
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }
}