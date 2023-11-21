package uk.co.odinconsultants.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class ArrowSmokeTest {

    /**
     * See https://arrow.apache.org/docs/java/ipc.html
     * If the allocated memory is not huge, we get:
     * Exception in thread "main" org.apache.arrow.memory.OutOfMemoryException: Unable to allocate buffer of size 16384 due to memory limit. Current allocation: 1024
     * 	at org.apache.arrow.memory.BaseAllocator.buffer(BaseAllocator.java:310)
     */
    private static final int MAX_ALLOCATION = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        try (final RootAllocator allocator =
                     new RootAllocator(MAX_ALLOCATION)) {
            BitVector bitVector = new BitVector("boolean", allocator);
            VarCharVector varCharVector = new VarCharVector("varchar", allocator);
            for (int i = 0; i < 10; i++) {
                bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
                varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
                System.out.println("Successfully setSafe");
            }
            bitVector.setValueCount(10);
            varCharVector.setValueCount(10);

            List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
            List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
            VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

            try (
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    ArrowStreamWriter writer = new ArrowStreamWriter(root, /*DictionaryProvider=*/null, Channels.newChannel(out));
            ) {
                // ... do write into the ArrowStreamWriter
                writer.start();
// write the first batch
                writer.writeBatch();

// write another four batches.
                for (int i = 0; i < 4; i++) {
                    // populate VectorSchemaRoot data and write the second batch
                    BitVector childVector1 = (BitVector)root.getVector(0);
                    VarCharVector childVector2 = (VarCharVector)root.getVector(1);
                    childVector1.reset();
                    childVector2.reset();
                    // ... do some populate work here, could be different for each batch
                    writer.writeBatch();
                }

                writer.end();
            }

            // Else we get:
            // Exception in thread "main" java.lang.IllegalStateException: Memory was leaked by query. Memory leaked: (17472)
            //Allocator(ROOT) 0/17472/17504/1048576 (res/actual/peak/limit)
            // upon JVM finishing
            root.close();
        }
    }

}
