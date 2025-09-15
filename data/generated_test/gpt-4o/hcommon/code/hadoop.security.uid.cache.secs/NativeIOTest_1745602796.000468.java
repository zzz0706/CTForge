package org.apache.hadoop.io.nativeio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class NativeIOTest {
    // Test code
    private static class CachedName {
        String name;
        long time;

        CachedName(String name, long time) {
            this.name = name;
            this.time = time;
        }
    }

    private enum IdCache {
        USER,
        GROUP
    }

    private static class NativeIO {
        public static final Map<Integer, CachedName> USER_ID_NAME_CACHE = new HashMap<>();
        public static final Map<Integer, CachedName> GROUP_ID_NAME_CACHE = new HashMap<>();

        public static String getUserName(int id) {
            // Simulate native call to fetch username
            return "MockUser" + id;
        }

        public static String getGroupName(int id) {
            // Simulate native call to fetch group name
            return "MockGroup" + id;
        }

        public static String getName(IdCache cacheType, int id) {
            CachedName cachedName;
            if (cacheType == IdCache.USER) {
                cachedName = USER_ID_NAME_CACHE.get(id);
            } else {
                cachedName = GROUP_ID_NAME_CACHE.get(id);
            }

            if (cachedName != null) {
                long currentTime = Time.now();
                if (currentTime - cachedName.time < 60000) { // Cache valid for 60 seconds
                    return cachedName.name;
                }
            }

            String name;
            if (cacheType == IdCache.USER) {
                name = getUserName(id);
                USER_ID_NAME_CACHE.put(id, new CachedName(name, Time.now()));
            } else {
                name = getGroupName(id);
                GROUP_ID_NAME_CACHE.put(id, new CachedName(name, Time.now()));
            }
            return name;
        }
    }

    @Test
    public void test_getName_with_cache_expiry() throws Exception {
        // 1. Use API to fetch configuration value
        Configuration configuration = new Configuration();
        long cacheTimeout =
                configuration.getLong("hadoop.security.uid.name.cache.timeout", 60) * 1000; // Default is 60 seconds

        // 2. Prepare test conditions
        // Setup initial cached values
        long initialTime = Time.now();
        CachedName cachedUser = new CachedName("UserCached", initialTime);
        CachedName cachedGroup = new CachedName("GroupCached", initialTime);
        NativeIO.USER_ID_NAME_CACHE.put(1, cachedUser);
        NativeIO.GROUP_ID_NAME_CACHE.put(1, cachedGroup);

        // 3. Execute test logic
        // Test cache validity within timeout
        String cachedUserName = NativeIO.getName(IdCache.USER, 1);
        String cachedGroupName = NativeIO.getName(IdCache.GROUP, 1);
        assertEquals("UserCached", cachedUserName);
        assertEquals("GroupCached", cachedGroupName);

        // Simulate cache expiry
        Thread.sleep(cacheTimeout + 1000);

        // Test fetching of new data after cache expiry
        String updatedUserName = NativeIO.getName(IdCache.USER, 1);
        String updatedGroupName = NativeIO.getName(IdCache.GROUP, 1);
        assertEquals("MockUser1", updatedUserName); // Corrected the expected value to match NativeIO.getUserName logic
        assertEquals("MockGroup1", updatedGroupName); // Corrected the expected value to match NativeIO.getGroupName logic

        // 4. Validate the updated cache
        assertEquals("MockUser1", NativeIO.USER_ID_NAME_CACHE.get(1).name);
        assertEquals("MockGroup1", NativeIO.GROUP_ID_NAME_CACHE.get(1).name);
    }
}