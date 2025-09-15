package alluxio.worker.block.allocator;

import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMetadataView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.util.CommonUtils;
import org.junit.Assert;
import org.junit.Test;

public class AllocatorTest {
    // Test case: test_createNewClassInstance_with_missing_constructor
    // Objective: Verify that CommonUtils.createNewClassInstance throws a RuntimeException when
    // attempting to instantiate a class with a missing constructor.

    // A class implementing the Allocator interface but explicitly missing required constructors and implementation details.
    public static class InvalidAllocatorTest implements Allocator {
        // Intentionally leaving out a constructor that accepts BlockMetadataView
        // and not implementing the required functionality as this class is meant
        // to simulate an invalid scenario.

        @Override
        public alluxio.worker.block.meta.StorageDirView allocateBlockWithView(long sessionId, long blockSize,
                                                                             BlockStoreLocation location, BlockMetadataView view) {
            throw new UnsupportedOperationException("This method is not implemented.");
        }
    }

    @Test
    public void test_createNewClassInstance_with_missing_constructor() {
        // Prepare the test conditions.
        BlockMetadataView view = null; // Using null as no valid BlockMetadataView is required for this test.

        try {
            // Test code: Attempt to create an instance of InvalidAllocatorTest using CommonUtils.
            @SuppressWarnings("unchecked")
            Class<? extends Allocator> invalidClass =
                (Class<? extends Allocator>) InvalidAllocatorTest.class; // Correctly cast to Allocator type.

            // Use CommonUtils to attempt to instantiate the class with a constructor expecting BlockMetadataView.
            CommonUtils.createNewClassInstance(
                invalidClass,
                new Class[] {BlockMetadataView.class}, // Expecting a constructor with this argument.
                new Object[] {view} // Passing the required argument (null in this case).
            );

            // If no exception is thrown, fail the test.
            Assert.fail("Expected RuntimeException to be thrown due to missing constructor.");
        } catch (RuntimeException e) {
            // Assert exception and validate its cause, ensuring the behavior is as expected.
            Assert.assertTrue(e.getMessage().contains("NoSuchMethodException")
                || (e.getCause() instanceof NoSuchMethodException));
        }
    }
}