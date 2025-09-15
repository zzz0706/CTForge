package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.junit.Test;
import static org.junit.Assert.*;
import com.google.common.cache.CacheBuilder;

import java.util.Map;
import java.util.List;

public class TestGroupsConfiguration {

    /**
     * Test to verify static mapping parsing without any overrides configured.
     */
    @Test
    public void testParseStaticMappingWithoutOverrides() {
        // Step 1: Initialize the configuration without any overrides.
        Configuration conf = new Configuration();
        // Ensure no property is set for static mapping.
        conf.unset(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES);

        // Step 2: Instantiate the Groups class to trigger static mapping parsing.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 3: Verify no mappings were parsed (staticMapRef should be null).
        assertNull("Expected staticMapRef to be null when no overrides are configured.", groups.getStaticMap());
    }

    /**
     * Test to verify correct parsing of valid static mapping overrides.
     */
    @Test
    public void testParseStaticMappingWithValidOverrides() {
        // Step 1: Initialize the configuration with valid mappings.
        Configuration conf = new Configuration();
        String validMapping = "user1=group1,group2;user2=group3";
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, validMapping);

        // Step 2: Instantiate the Groups class to trigger static mapping parsing.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 3: Validate that the mappings are parsed correctly.
        Map<String, List<String>> staticMapping = groups.getStaticMap();
        assertNotNull("Expected staticMapRef to contain parsed mappings for valid overrides.", staticMapping);
        assertTrue("Expected user1 to be mapped to group1 and group2.", staticMapping.containsKey("user1"));
        assertEquals("Expected group1 and group2 for user1.", List.of("group1", "group2"), staticMapping.get("user1"));
        assertTrue("Expected user2 to be mapped to group3.", staticMapping.containsKey("user2"));
        assertEquals("Expected group3 for user2.", List.of("group3"), staticMapping.get("user2"));
    }

    /**
     * Test to verify parsing behavior with malformed overrides.
     */
    @Test
    public void testParseStaticMappingWithMalformedOverrides() {
        // Step 1: Initialize the configuration with malformed mapping values.
        Configuration conf = new Configuration();
        String malformedMapping = "user1group1,user2=invalid"; // Missing '=' delimiter.
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, malformedMapping);

        // Step 2: Attempt to instantiate Groups. Expect an exception.
        Timer timer = new Timer();
        assertThrows("Expected HadoopIllegalArgumentException for malformed configuration.",
                org.apache.hadoop.util.HadoopIllegalArgumentException.class,
                () -> new Groups(conf, timer));
    }

    /**
     * Test to verify that the default behavior is intact when no overrides are set.
     */
    @Test
    public void testDefaultBehaviorWhenOverridesNotConfigured() {
        // Step 1: Create a configuration without static mapping overrides.
        Configuration conf = new Configuration();

        // Step 2: Instantiate Groups and validate default static mappings (null expected).
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 3: Verify that the static mappings are null by default.
        assertNull("Expected default behavior when overrides are not configured.", groups.getStaticMap());
    }

    /**
     * Test derived behavior and ensure cache initialization integrates correctly with parsed mappings.
     */
    @Test
    public void testCacheIntegrationWithStaticMapping() {
        // Step 1: Initialize a configuration with valid static mapping overrides.
        Configuration conf = new Configuration();
        String validMapping = "userA=groupX,groupY;userB=groupZ";
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, validMapping);

        // Step 2: Instantiate Groups to cumulatively test cache initialization along with parsed mappings.
        Timer timer = new Timer();
        Groups groups = new Groups(conf, timer);

        // Step 3: Validate cache and static parsing integration.
        assertNotNull("Cache should be initialized.", groups.getCache());
        Map<String, List<String>> staticMapping = groups.getStaticMap();
        assertNotNull("Expected cache integration with parsed mappings.", staticMapping);
        assertTrue("Expected userA mapping.", staticMapping.containsKey("userA"));
        assertTrue("Expected userB mapping.", staticMapping.containsKey("userB"));
    }
}