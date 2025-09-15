package org.apache.hadoop.hdfs;

import org.junit.Test;
import org.apache.hadoop.conf.Configuration;

import static org.junit.Assert.*;

public class TestDFSXFrameOptionConfiguration {

    @Test
    public void testDFSXFrameOptionConfiguration() {
        // Step 1: Create a Configuration object to load configurations
        Configuration conf = new Configuration();

        try {
            // Step 2: Read the configuration values
            boolean xFrameEnabled = conf.getBoolean(
                DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED,
                DFSConfigKeys.DFS_XFRAME_OPTION_ENABLED_DEFAULT);

            String xFrameOptionValue = conf.getTrimmed(
                DFSConfigKeys.DFS_XFRAME_OPTION_VALUE, 
                DFSConfigKeys.DFS_XFRAME_OPTION_VALUE_DEFAULT);

            // Step 3: Validate configuration dfs.xframe.value based on constraints:
            // - Acceptable values are "DENY", "SAMEORIGIN", and "ALLOW-FROM"
            // - If dfs.xframe.enabled is false, dfs.xframe.value does not matter.
            if (xFrameEnabled) {
                assertNotNull("The value of dfs.xframe.value cannot be null when dfs.xframe.enabled is true!", xFrameOptionValue);

                assertTrue("Invalid dfs.xframe.value! Acceptable values are DENY, SAMEORIGIN, and ALLOW-FROM.",
                    xFrameOptionValue.equals("DENY") || 
                    xFrameOptionValue.equals("SAMEORIGIN") || 
                    xFrameOptionValue.equals("ALLOW-FROM"));
            } else {
                // If xFrameEnabled is false, dfs.xframe.value can be ignored for additional validation.
                // However, sanity check for nullable cases can still apply.
                if (xFrameOptionValue != null && !xFrameOptionValue.isEmpty()) {
                    assertTrue("Invalid dfs.xframe.value even though dfs.xframe.enabled is false! Acceptable values are DENY, SAMEORIGIN, and ALLOW-FROM.",
                        xFrameOptionValue.equals("DENY") || 
                        xFrameOptionValue.equals("SAMEORIGIN") || 
                        xFrameOptionValue.equals("ALLOW-FROM"));
                }
            }

        } catch (Exception e) {
            fail("An exception occurred during configuration validation: " + e.getMessage());
        }
    }
}