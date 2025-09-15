package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.HadoopIllegalArgumentException;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestGroupsConfiguration {

    @Test
    public void testGroupsConstructorWithBackgroundReloadConfiguration() {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        conf.setInt("hadoop.security.groups.cache.background.reload.threads", 5);

        // Create a mock Timer object.
        Groups.Timer timer = new Groups.Timer(new AtomicLong(0));

        // Instantiate the Groups class using the configuration and the timer.
        Groups groups = new Groups(conf, timer);

        // Test code: Verify the background reload thread count is correctly initialized.
        assertEquals(5, groups.getReloadGroupsThreadCount());
    }

    @Test
    public void testParseStaticMappingConfiguration() {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        conf.set("hadoop.user.group.static.mapping.overrides", "user1=group1;user2=group2,group3");

        // Create a mock Timer object.
        Groups.Timer timer = new Groups.Timer(new AtomicLong(0));

        // Instantiate the Groups class to trigger static mapping parsing.
        Groups groups = new Groups(conf, timer);

        // Test code: Retrieve the parsed static user-to-groups map.
        Map<String, List<String>> staticMapRef = groups.getStaticUserToGroupsMap();
        
        // Verify the parsed mappings.
        assertNotNull(staticMapRef);
        assertEquals(2, staticMapRef.size());
        assertEquals(List.of("group1"), staticMapRef.get("user1"));
        assertEquals(List.of("group2", "group3"), staticMapRef.get("user2"));
    }

    @Test
    public void testInvalidStaticMappingConfiguration() {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        conf.set("hadoop.user.group.static.mapping.overrides", "invalid_mapping");

        // Create a mock Timer object.
        Groups.Timer timer = new Groups.Timer(new AtomicLong(0));
        
        // Test code: Attempt to instantiate the Groups class and expect an exception.
        try {
            new Groups(conf, timer);
            fail("Expected HadoopIllegalArgumentException was not thrown.");
        } catch (HadoopIllegalArgumentException e) {
            // Assert exception message indicates invalid configuration.
            assertTrue(e.getMessage().contains("Configuration hadoop.user.group.static.mapping.overrides is invalid"));
        }
    }

    @Test
    public void testParseStaticMappingDefaultConfiguration() {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();

        // Create a mock Timer object.
        Groups.Timer timer = new Groups.Timer(new AtomicLong(0));
        
        // Instantiate the Groups class to parse static mapping with default configuration.
        Groups groups = new Groups(conf, timer);
        
        // Test code: Retrieve a static user-to-groups map.
        Map<String, List<String>> staticMapRef = groups.getStaticUserToGroupsMap();
        
        // Assert the default configuration does not define any static mappings by default.
        assertNull(staticMapRef);
    }

    @Test
    public void testStaticMappingPropagationAndUsage() {
        // Prepare the input conditions for unit testing.
        Configuration conf = new Configuration();
        conf.set("hadoop.user.group.static.mapping.overrides", "testuser=testgroup");

        // Create a mock Timer object.
        Groups.Timer timer = new Groups.Timer(new AtomicLong(0));
        
        // Instantiate the Groups class using the configuration with static mappings.
        Groups groups = new Groups(conf, timer);
        
        // Test code: Verify group resolution for a user with static mapping.
        List<String> groupsResolved = groups.getGroups("testuser");
        assertEquals(List.of("testgroup"), groupsResolved);
        
        // Verify group resolution for a user without static mapping.
        List<String> defaultGroups = groups.getGroups("unknownuser");
        assertNotNull(defaultGroups);
        assertTrue(defaultGroups.isEmpty());
    }
}