package uk.co.odinconsultants.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

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
//    private static final int MAX_ALLOCATION = 1024;

    public static void main(String[] args) {
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
            root.close();
        }
    }

}
