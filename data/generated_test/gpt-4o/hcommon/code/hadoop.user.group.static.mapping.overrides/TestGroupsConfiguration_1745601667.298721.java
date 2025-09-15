package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class TestGroupsConfiguration {

    // Prepare the input conditions for unit testing.
    @Test
    public void testGroupsConstructorWithBackgroundReloadConfiguration() {
        // Create a Configuration object to retrieve the configuration values using the API.
        Configuration conf = new Configuration();

        // Get the configured value for background reload threads.
        int reloadThreads = conf.getInt(
                "hadoop.security.groups.cache.background.reload.threads",
                1 // Default value if the property is not explicitly set.
        );

        // Create a mock Timer object.
        Groups.Timer timer = new Groups.Timer(new AtomicLong(0));

        // Instantiate the Groups class using the configuration and the timer.
        Groups groups = new Groups(conf, timer);

        // Test assertions: Verify that the background reload thread count is initialized correctly.
        assertEquals(reloadThreads, groups.getReloadGroupsThreadCount());
    }
}