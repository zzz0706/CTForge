package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class GroupsTest {

    @Test
    public void verifyCustomMultipleUsersMultipleGroupsMapping() throws Exception {
        // 1. Use Configuration to obtain the value
        Configuration conf = new Configuration();
        // 2. Prepare the test conditions
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
                "bob=wheel,users;charlie=admin,hdfs");

        // 3. Test code
        Groups groups = new Groups(conf);

        // Read staticMapRef via reflection
        Field staticMapRefField = Groups.class.getDeclaredField("staticMapRef");
        staticMapRefField.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<Map<String, List<String>>> staticMapRef =
                (AtomicReference<Map<String, List<String>>>) staticMapRefField.get(groups);
        Map<String, List<String>> staticMap = staticMapRef.get();

        // 4. Code after testing
        assertEquals(2, staticMap.size());
        assertEquals(Arrays.asList("wheel", "users"), staticMap.get("bob"));
        assertEquals(Arrays.asList("admin", "hdfs"), staticMap.get("charlie"));
    }
}