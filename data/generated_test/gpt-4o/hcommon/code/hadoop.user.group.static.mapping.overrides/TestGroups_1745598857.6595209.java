package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.*;

public class TestGroups {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testGroupsConstructorWithMalformedStaticMapping() {
        // Prepare configuration with malformed static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1==group1;user2;;user3");

        // Verify that the Groups constructor throws the specific exception
        Exception exception = assertThrows(HadoopIllegalArgumentException.class, () -> {
            new Groups(conf, GenericTestUtils.getSourceForTest());
        });

        // Check the exception message to ensure correctness
        String expectedMessage = "Configuration " + CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES + " is invalid";
        assertTrue(exception.getMessage().contains(expectedMessage));
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testCorrectStaticMappingParsing() {
        // Prepare configuration with correctly formatted static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1=group1,group2;user2=group3");

        // Instantiate Groups using a valid configuration
        Groups groups = new Groups(conf, GenericTestUtils.getSourceForTest());

        // Validate that the static mapping was parsed correctly
        Map<String, List<String>> expectedMapping = new HashMap<>();
        expectedMapping.put("user1", Arrays.asList("group1", "group2"));
        expectedMapping.put("user2", Arrays.asList("group3"));
        
        // Access the parsed static mapping to verify correctness (using reflection or public access method if available)
        Map<String, List<String>> actualMapping = groups.getStaticUserToGroupsMapping(); // Example for public method assumption
        assertEquals(expectedMapping, actualMapping);
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testGroupsConstructorWithDefaultTimer() {
        // Prepare configuration with valid static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1=group1;user2=group2");

        // Instantiate Groups using the default timer
        Timer defaultTimer = new Timer();
        Groups groups = new Groups(conf, defaultTimer);

        // Validate configuration propagation, check if the timer is correctly assigned
        assertNotNull(groups);
        assertEquals(defaultTimer, groups.getTimer()); // Example for public method assumption
    }
}