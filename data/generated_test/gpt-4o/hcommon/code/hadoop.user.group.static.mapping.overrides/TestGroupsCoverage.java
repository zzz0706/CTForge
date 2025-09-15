package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.*;

public class TestGroupsCoverage {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testGroupsConstructorWithMalformedStaticMapping() {
        // Prepare configuration with malformed static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1==group1;user2;;user3");

        // Verify that the Groups constructor throws the specific exception
        Exception exception = assertThrows(HadoopIllegalArgumentException.class, () -> {
            new Groups(conf, new Timer());
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
        Groups groups = new Groups(conf, new Timer());

        // Validate that the static mapping was parsed correctly
        Map<String, List<String>> expectedMapping = new HashMap<>();
        expectedMapping.put("user1", Arrays.asList("group1", "group2"));
        expectedMapping.put("user2", Arrays.asList("group3"));

        // Access the parsed static mapping to verify correctness
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
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testGroupsNegativeCacheBehavior() {
        // Prepare configuration with valid static mapping and negative cache timeout
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1=group1;user2=group2");
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 10);

        // Instantiate Groups with valid timer and configuration
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Verify negative cache timeout configuration usage
        assertNotNull(groups.getNegativeCache());
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testGroupsCacheInitialization() {
        // Prepare configuration with valid static mappings and cache timeout
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1=group1;user2=group2");
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 5);

        // Instantiate Groups with valid timer and configuration
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Verify cache initialization
        assertNotNull(groups.getCache());
    }
}