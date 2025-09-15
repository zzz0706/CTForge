package alluxio.master.file;

import static org.junit.Assert.assertTrue;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MasterWhitelistValidationTest {

  private static final PropertyKey WHITELIST_KEY = PropertyKey.MASTER_WHITELIST;

  @Before
  public void setUp() {
    // Reset the configuration to defaults before each test
    ServerConfiguration.reset();
  }

  @After
  public void tearDown() {
    // Clean up after each test
    ServerConfiguration.reset();
  }

  @Test
  public void testDefaultWhitelistIsValid() {
    // 1. Obtain configuration value from alluxio2.1.0 API
    List<String> whitelist = ServerConfiguration.getList(WHITELIST_KEY, ",");

    // 2. Prepare test conditions
    // Default value is "/" which should be valid

    // 3. Test code
    assertTrue("Default whitelist should contain /", whitelist.contains("/"));
  }

  @Test
  public void testEmptyWhitelistIsValid() {
    // 1. Obtain configuration value from alluxio2.1.0 API
    ServerConfiguration.set(WHITELIST_KEY, ",");
    List<String> whitelist = ServerConfiguration.getList(WHITELIST_KEY, ",");

    // 2. Prepare test conditions
    // Empty string should result in empty list

    // 3. Test code
    assertTrue("Empty whitelist should be valid", whitelist.isEmpty());
  }

  @Test
  public void testCommaSeparatedPathsAreValid() {
    // 1. Obtain configuration value from alluxio2.1.0 API
    ServerConfiguration.set(WHITELIST_KEY, "/data,/tmp,/user");
    List<String> whitelist = ServerConfiguration.getList(WHITELIST_KEY, ",");

    // 2. Prepare test conditions
    List<String> expectedPaths = Arrays.asList("/data", "/tmp", "/user");

    // 3. Test code
    assertTrue("Paths should be correctly parsed", whitelist.containsAll(expectedPaths));
    assertTrue("Path count should match", whitelist.size() == expectedPaths.size());
  }

  @Test
  public void testWhitespaceHandlingIsValid() {
    // 1. Obtain configuration value from alluxio2.1.0 API
    ServerConfiguration.set(WHITELIST_KEY, " /data , /tmp , /user ");
    List<String> whitelist = ServerConfiguration.getList(WHITELIST_KEY, ",");

    // 2. Prepare test conditions
    // Whitespace around paths should be trimmed

    // 3. Test code
    assertTrue("Whitespace should be trimmed", whitelist.contains("/data"));
    assertTrue("Whitespace should be trimmed", whitelist.contains("/tmp"));
    assertTrue("Whitespace should be trimmed", whitelist.contains("/user"));
  }

  @Test
  public void testPathValidation() {
    // 1. Obtain configuration value from alluxio2.1.0 API
    ServerConfiguration.set(WHITELIST_KEY, "/valid/path,/another/valid/path");
    List<String> whitelist = ServerConfiguration.getList(WHITELIST_KEY, ",");

    // 2. Prepare test conditions
    // All paths should be absolute (start with "/")

    // 3. Test code
    for (String path : whitelist) {
      assertTrue("Path should be absolute: " + path, path.startsWith("/"));
    }
  }

  @Test
  public void testSpecialCharactersInPaths() {
    // 1. Obtain configuration value from alluxio2.1.0 API
    ServerConfiguration.set(WHITELIST_KEY, "/path-with-dash,/path_with_underscore,/path.with.dot");
    List<String> whitelist = ServerConfiguration.getList(WHITELIST_KEY, ",");

    // 2. Prepare test conditions
    // Special characters should be preserved

    // 3. Test code
    assertTrue("Should handle dash in path", whitelist.contains("/path-with-dash"));
    assertTrue("Should handle underscore in path", whitelist.contains("/path_with_underscore"));
    assertTrue("Should handle dot in path", whitelist.contains("/path.with.dot"));
  }

  // 4. Code after testing handled in tearDown()
}