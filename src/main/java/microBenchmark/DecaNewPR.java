package microBenchmark;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by zx on 16-4-8.
 */
public class DecaNewPR extends PR {

    public DecaNewPR() {
        name = "DecaNewPR";
    }

    static Unsafe unsafe;

    static {
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
        } catch (Exception e) {
            System.out.println("init error: " + e);
        }
    }

    class Chunk {
        int size;
        long address;

        Chunk(int size) {
            this.size = size;
            address = unsafe.allocateMemory(size);
        }

        public void free() {
            unsafe.freeMemory(address);
        }

        public void putValue(int value, long offset) {
            unsafe.putInt(offset + address, value);
        }

        public IntDoubleMap getRanks() {
            IntDoubleMap ranks = new IntDoubleMap(keyCount);
            long offset = address;
            while (offset < size + address) {
                int key = unsafe.getInt(offset);
                ranks.put(key, 1.0);
                offset += 4;
                int length = unsafe.getInt(offset);
                offset += 4 * (length + 1);
            }
            return ranks;
        }

        public IntPairMap getJoin(IntDoubleMap ranks) {
            IntPairMap joinMap = new IntPairMap(idCount);
            long offset = address;
            long limit = size + address;
            while (offset < limit) {
                int key = unsafe.getInt(offset);
                offset += 4;
                joinMap.put2(key, offset);
                int length = unsafe.getInt(offset);
                offset += 4 * (length + 1);
            }
            long rankAddress = ranks.address();
            int index = 0;
            while (index < ranks.numElements()) {
                int key = unsafe.getInt(rankAddress);
                rankAddress += ranks.kSize();
                double value = unsafe.getDouble(rankAddress);
                rankAddress += ranks.vSize();
                joinMap.put1(key, value * 0.85 + 0.15);
                index += 1;
            }
            ranks.free();
            return joinMap;
        }

        public IntDoubleMap getReduce(IntPairMap joinMap) {
            IntDoubleMap reduce = new IntDoubleMap(idCount);
            long joinAddress = joinMap.address();
            int index = 0;
            while (index < joinMap.numElements()) {
                joinAddress += joinMap.kSize();
                double value = unsafe.getDouble(joinAddress);
                joinAddress += joinMap.v1Size();
                long listAddress = unsafe.getLong(joinAddress);
                joinAddress += joinMap.v2Size();
                if (value >= 0 && listAddress >= 0) {
                    int length = unsafe.getInt(listAddress);
                    double newValue = value / length;
                    listAddress += 4;
                    for (int i = 0; i < length; i++) {
                        int newKey = unsafe.getInt(listAddress);
                        listAddress += 4;
                        double oldValue = reduce.get(newKey);
                        if (oldValue < 0) {
                            reduce.put(newKey, newValue);
                        } else {
                            reduce.put(newKey, newValue + oldValue);
                        }
                    }
                }
                index += 1;
            }
            joinMap.free();
            return reduce;
        }

    }

    Chunk cacheChunk;

    @Override
    protected void cache(Map<Integer, ArrayList<Integer>> links) {
        long startTime = System.currentTimeMillis();
        cacheChunk = new Chunk(count * 4 + keyCount * 4);
        int offset = 0;
        for (Map.Entry<Integer, ArrayList<Integer>> entry : links.entrySet()) {
            int key = entry.getKey();
            cacheChunk.putValue(key, offset);
            offset += 4;
            ArrayList<Integer> values = entry.getValue();
            cacheChunk.putValue(values.size(), offset);
            offset += 4;
            for (int i = 0; i < values.size(); i++) {
                cacheChunk.putValue(values.get(i), offset);
                offset += 4;
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Deca cache bytes time: " + (endTime - startTime) + "ms");
    }

    @Override
    public void compute(int iterations) {
        IntDoubleMap ranks = cacheChunk.getRanks();
        for (int i = 0; i < iterations; i++) {
            IntPairMap joinMap = cacheChunk.getJoin(ranks);
            ranks = cacheChunk.getReduce(joinMap);
        }
        //System.out.println(ranks);
    }

    @Override
    public void close() {
        cacheChunk.free();
        super.close();
    }
}
