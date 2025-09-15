package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test class for validating configuration constraints and dependencies of 
 * `fs.trash.checkpoint.interval` and `fs.trash.interval` in Hadoop Common 2.8.5.
 */
public class TestTrashConfigurationValidation {

    @Test
    public void testTrashCheckpointIntervalConfiguration() {
        // Step 1: Create a new Hadoop Configuration instance to load configuration values
        Configuration conf = new Configuration();

        // Step 2: Read configuration values from the configuration and calculate the intervals
        long fsTrashInterval = (long) (conf.getFloat(
            CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY,
            CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT) 
            * 60 * 1000);  // Convert minutes to milliseconds
        
        long fsTrashCheckpointInterval = (long) (conf.getFloat(
            CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY,
            CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT)
            * 60 * 1000);  // Convert minutes to milliseconds

        // Step 3: Validate constraints and dependencies between `fs.trash.checkpoint.interval` and `fs.trash.interval`
        // Constraint: `fs.trash.checkpoint.interval` <= `fs.trash.interval`
        Assert.assertTrue(
            "fs.trash.checkpoint.interval must be less than or equal to fs.trash.interval",
            fsTrashCheckpointInterval <= fsTrashInterval
        );

        // Constraint: `fs.trash.checkpoint.interval` must be greater than 0 if trash is enabled
        if (fsTrashInterval > 0) {
            Assert.assertTrue(
                "fs.trash.checkpoint.interval must be greater than 0 when trash is enabled",
                fsTrashCheckpointInterval > 0
            );
        }
    }
}