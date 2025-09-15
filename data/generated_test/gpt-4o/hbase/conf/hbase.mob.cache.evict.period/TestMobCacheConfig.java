import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({MiscTests.class, SmallTests.class})
public class TestMobCacheConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMobCacheConfig.class);

    @Test
    public void testMobCacheEvictPeriodDefaultValue() {

        Configuration configuration = HBaseConfiguration.create();

        long mobCacheEvictPeriod = configuration.getLong(
                "hbase.mob.cache.evict.period", 3600);

        assertEquals(3600, mobCacheEvictPeriod);
    }
}