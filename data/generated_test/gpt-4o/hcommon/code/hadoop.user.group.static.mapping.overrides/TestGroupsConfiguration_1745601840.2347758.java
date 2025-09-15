package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;

import static org.junit.Assert.*;

public class TestGroupsConfiguration {
    // Prepare the input conditions for unit testing.

    @Test
    // Test the Groups constructor with background reload configuration.
    public void testGroupsConstructorWithBackgroundReloadConfiguration() {
        // Create a Configuration object to retrieve the configuration values using the API.
        Configuration conf = new Configuration();
        conf.setInt("hadoop.security.groups.cache.background.reload.threads", 5);

        // Create a mock Timer object.
        Groups.Timer timer = new Groups.Timer(new AtomicLong(0));

        // Instantiate the Groups class using the configuration and the timer.
        Groups groups = new Groups(conf, timer);

        // Test assertions: Verify that the background reload thread count is initialized correctly.
        assertEquals(5, groups.getReloadGroupsThreadCount());
    }

    @Test
    // Test the parsing of static user-to-group mapping overrides in configuration.
    public void testParseStaticMappingConfiguration() {
        // Create a Configuration object and set the user-group override property.
        Configuration conf = new Configuration();
        conf.set("hadoop.user.group.static.mapping.overrides", "user1=group1;user2=group2,group3");

        // Create a mock Timer object.
        Groups.Timer timer = new Groups.Timer(new AtomicLong(0));

        // Instantiate the Groups class to trigger static mapping parsing.
        Groups groups = new Groups(conf, timer);

        // Retrieve the parsed static user-to-groups map.
        Map<String, List<String>> staticMapRef = groups.getStaticUserToGroupsMap();

        // Test assertions: Verify the parsed mappings.
        assertNotNull(staticMapRef);
        assertEquals(2, staticMapRef.size());
        assertEquals(List.of("group1"), staticMapRef.get("user1"));
        assertEquals(List.of("group2", "group3"), staticMapRef.get("user2"));
    }

    @Test
    // Test exception handling for invalid static mapping configuration.
    public void testInvalidStaticMappingConfiguration() {
        // Create a Configuration object and set an invalid user-group override property.
        Configuration conf = new Configuration();
        conf.set("hadoop.user.group.static.mapping.overrides", "invalid_mapping");

        // Create a mock Timer object.
        Groups.Timer timer = new Groups.Timer(new AtomicLong(0));

        // Attempt to instantiate the Groups class and verify that it throws a HadoopIllegalArgumentException.
        try {
            new Groups(conf, timer);
            fail("Expected HadoopIllegalArgumentException was not thrown.");
        } catch (HadoopIllegalArgumentException e) {
            // Test assertions: Verify the exception message.
            assertTrue(e.getMessage().contains("Configuration hadoop.user.group.static.mapping.overrides is invalid"));
        }
    }
}