package org.apache.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.bloom.Filter; // use the interface to detect bloom filters
import org.junit.Test;

import java.io.Closeable;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BloomMapFileConfigTest {

  @Test
  public void verifyBloomFilterVectorSizeDerivedFromConfig() throws Exception {
    // 1) Read from Configuration (no hard-coded constants in assertions)
    Configuration conf = new Configuration();

    float errorRate = conf.getFloat(
        CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_ERROR_RATE_KEY,
        CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_ERROR_RATE_DEFAULT);

    int numKeys = conf.getInt("io.mapfile.bloom.size", 1024 * 1024);

    // 2) Prepare a clean working directory
    Path dir = new Path("target/test-bloom-mapfile");
    FileSystem fs = FileSystem.getLocal(conf);
    if (fs.exists(dir)) {
      fs.delete(dir, true);
    }

    BloomMapFile.Writer writer = null;
    try {
      // 3) Construct writer
      writer = new BloomMapFile.Writer(
          conf,
          dir,
          BloomMapFile.Writer.keyClass(Text.class),
          BloomMapFile.Writer.valueClass(Text.class)
      );

      // IMPORTANT: many versions lazily create the bloom filter on first append
      for (int i = 0; i < 8; i++) {
        writer.append(new Text(String.format("%06d", i)), new Text("v" + i));
      }

      // 4) Find the internal Bloom Filter instance (no assumptions on concrete class name)
      Object bloom = findBloomFilterDeep(writer);
      if (!(bloom instanceof Filter)) {
        throw new IllegalStateException("Found bloom-like object is not a Filter: " +
            (bloom == null ? "null" : bloom.getClass()));
      }

      int actualVectorSize = getIntField(bloom, "vectorSize");
      int k = getIntField(bloom, "nbHash");

      assertTrue("nbHash (k) must be positive", k > 0);
      assertTrue("vectorSize must be positive", actualVectorSize > 0);

      // 5) Compute the expected vector size from config and the actual k
      // m = ceil( (-k * n) / ln(1 - p^(1/k)) )
      double expectedVectorSizeD = Math.ceil(
          (-k * (double) numKeys) /
              Math.log(1.0 - Math.pow(errorRate, 1.0 / k))
      );
      int expectedVectorSize = (int) expectedVectorSizeD;

      // 6) Assert: vector size equals the one implied by config (no hard-coded numbers)
      assertEquals("BloomFilter vectorSize should be derived from config values",
          expectedVectorSize, actualVectorSize);

      // Optional sanity check: errorRate indeed comes from Configuration
      float readBack = conf.getFloat(
          CommonConfigurationKeysPublic.IO_MAPFILE_BLOOM_ERROR_RATE_KEY,
          Float.NaN
      );
      assertEquals("errorRate should come from Configuration", errorRate, readBack, 0.0f);

    } finally {
      closeQuietly(writer);
      if (fs.exists(dir)) {
        fs.delete(dir, true);
      }
    }
  }

  /** Depth-2 scan: look for any field on writer (or its direct children) that is a bloom Filter. */
  private static Object findBloomFilterDeep(Object writer) throws Exception {
    // First, scan writer's own fields
    Object f = scanForFilter(writer);
    if (f != null) return f;

    // Next, scan one more level: for each non-null field on writer, scan its fields
    for (Field fld : writer.getClass().getDeclaredFields()) {
      fld.setAccessible(true);
      Object child = fld.get(writer);
      if (child == null) continue;
      Object nested = scanForFilter(child);
      if (nested != null) return nested;
    }

    throw new IllegalStateException("Cannot locate a org.apache.hadoop.util.bloom.Filter in BloomMapFile.Writer");
  }

  /** Scan all declared fields of the given object and return the first that is a bloom Filter. */
  private static Object scanForFilter(Object obj) throws Exception {
    Class<?> cls = obj.getClass();
    while (cls != null) {
      for (Field f : cls.getDeclaredFields()) {
        f.setAccessible(true);
        Object val = f.get(obj);
        if (val instanceof Filter) {
          return val;
        }
      }
      cls = cls.getSuperclass();
    }
    return null;
  }

  /** Read an int field by name from an object via reflection (searching up the class hierarchy). */
  private static int getIntField(Object obj, String fieldName) throws Exception {
    Class<?> cls = obj.getClass();
    while (cls != null) {
      try {
        Field f = cls.getDeclaredField(fieldName);
        f.setAccessible(true);
        return (int) f.get(obj);
      } catch (NoSuchFieldException e) {
        cls = cls.getSuperclass();
      }
    }
    throw new NoSuchFieldException("Field not found: " + fieldName + " on " + obj.getClass());
  }

  private static void closeQuietly(Closeable c) {
    if (c != null) {
      try { c.close(); } catch (Exception ignored) {}
    }
  }
}
