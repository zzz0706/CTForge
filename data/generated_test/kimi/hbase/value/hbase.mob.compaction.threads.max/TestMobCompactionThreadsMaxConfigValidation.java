package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestMobCompactionThreadsMaxConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobCompactionThreadsMaxConfigValidation.class);

  @Test
  public void testMobCompactionThreadsMaxConfigValueIsValid() {
    Configuration conf = new Configuration();
    // Do NOT set the value in test code; read from the actual runtime configuration
    int maxThreads = conf.getInt(MobConstants.MOB_COMPACTION_THREADS_MAX,
        MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX);

    // Constraint: must be a positive integer (MobUtils enforces >=1)
    assertTrue("hbase.mob.compaction.threads.max must be a positive integer",
        maxThreads > 0);
  }
}