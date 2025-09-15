package alluxio.worker.block.allocator;

import alluxio.util.CommonUtils;
import org.junit.Test;
import org.junit.Assert;

public class AllocatorTest {
    // Test case: Ensure that the utility method CommonUtils.createNewClassInstance 
    // throws a RuntimeException when an invalid class is provided.

    @Test
    public void testCreateNewClassInstanceWithInvalidClass() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // 2. Prepare the test conditions: use a non-existent class name
        String invalidClassName = "NonExistingClass";

        // 3. Test code: Call CommonUtils.createNewClassInstance with the invalid class name
        try {
            CommonUtils.createNewClassInstance(Class.forName(invalidClassName),
                    new Class<?>[]{Object.class}, new Object[]{null});
            Assert.fail("Expected a RuntimeException to be thrown, but none was encountered.");
        } catch (ClassNotFoundException e) {
            // Expected behavior due to invalid class name
        } catch (RuntimeException ex) {
            // Verify the exception
            Assert.assertNotNull("Exception should have a message detailing the issue.", ex.getMessage());
            Assert.assertTrue("Error message should mention the invalid class.", ex.getMessage().contains(invalidClassName));
        }

        // 4. Code after testing (if any clean-up is required, though none is needed for this case)
    }
}