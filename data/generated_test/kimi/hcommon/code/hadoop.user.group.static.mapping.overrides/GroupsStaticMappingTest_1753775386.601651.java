package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GroupsStaticMappingTest {

    @Test
    public void verifyUserWithEmptyGroupList() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "nobody=");

        // 2. Prepare the test conditions.
        // In Hadoop 2.8.5 the Groups class is NOT a singleton; each call to
        // Groups.getUserToGroupsMappingService returns a new instance, so
        // we can simply create a fresh instance with our configuration.
        Groups groups = Groups.getUserToGroupsMappingService(conf);

        // 3. Test code.
        // Access the private field `staticMapRef` inside the Groups instance.
        Field staticMapRefField = Groups.class.getDeclaredField("staticMapRef");
        staticMapRefField.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<Map<String, List<String>>> staticMapRef =
            (AtomicReference<Map<String, List<String>>>) staticMapRefField.get(groups);

        assertNotNull("staticMapRef should not be null", staticMapRef);

        Map<String, List<String>> staticMap = staticMapRef.get();
        assertNotNull("staticMap should not be null", staticMap);

        // 4. Code after testing.
        assertEquals("Static map should contain exactly one entry", 1, staticMap.size());
        assertEquals("User 'nobody' should map to empty list",
                     Collections.emptyList(), staticMap.get("nobody"));
    }
}