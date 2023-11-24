package uk.co.odinconsultants.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.GenerateSampleData;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

public class PingMain {

    public static final int MAX_ALLOCATION = 1024 * 1024;
    public static final String PING_FILE = "/dev/shm/ping";

    public static void main(String[] args) throws Exception {
        try (final RootAllocator allocator =
                     new RootAllocator(Integer.MAX_VALUE)) {
            List<Field> fields = new ArrayList<>();
            fields.add(new IntVector("int", allocator).getField());

            VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
            final int rowCount = 1;
            GenerateSampleData.generateTestData(root.getVector(0), rowCount);

            IntVector intVector = (IntVector)root.getVector(0);
            intVector.setSafe(0, 42);
            intVector.setValueCount(1);

            Utils.writeFile(root, PING_FILE);
            root.close();
            VectorSchemaRoot newRoot = Utils.readFile(allocator, PING_FILE);

            System.out.println("Finished");
        }
    }


}
