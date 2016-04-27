package microBenchmark;

import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.unsafe.map.BytesToBytesMap.Location;
import org.apache.spark.unsafe.memory.ExecutorMemoryManager;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.TaskMemoryManager;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Created by zx on 16-4-6.
 */
public class DecaJavaPR extends PR {

    public DecaJavaPR() {
        name = "DecaJavaPR";
    }

    MemoryAllocator memoryAllocator = MemoryAllocator.UNSAFE;
    ExecutorMemoryManager executorMemoryManager = new ExecutorMemoryManager(memoryAllocator);
    TaskMemoryManager taskMemoryManager = new TaskMemoryManager(executorMemoryManager);
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

    class UnsafeMap {
        long bufValue;
        BytesToBytesMap bytesToBytesMap;

        UnsafeMap(int keyCount) {
            this.bufValue = unsafe.allocateMemory(16);
            this.bytesToBytesMap = new BytesToBytesMap(taskMemoryManager, keyCount, false);
        }

        public void putDouble(long keyAddress, double rank) {
            Location location = bytesToBytesMap.lookup(null, keyAddress, 8);
            if (!location.isDefined()) {
                unsafe.putDouble(bufValue, rank);
                location.putNewKey(null, keyAddress, 8, null, bufValue, 8);
            } else {
                long valueAddress = location.getValueAddress().getBaseOffset();
                unsafe.putDouble(valueAddress, rank);
            }
        }

        public void putPairList(long keyAddress, long listAddress) {
            Location location = bytesToBytesMap.lookup(null, keyAddress, 8);
            if (!location.isDefined()) {
                unsafe.putDouble(bufValue, -1.0);
                unsafe.putLong(bufValue + 8, listAddress);
                location.putNewKey(null, keyAddress, 8, null, bufValue, 16);
            } else {
                long valueAddress = location.getValueAddress().getBaseOffset();
                unsafe.putLong(valueAddress + 8, listAddress);
            }
        }

        public void putPairRank(long keyAddress, double rank) {
            Location location = bytesToBytesMap.lookup(null, keyAddress, 8);
            if (!location.isDefined()) {
                unsafe.putDouble(bufValue, rank);
                unsafe.putLong(bufValue + 8, -1);
                location.putNewKey(null, keyAddress, 8, null, bufValue, 16);
            } else {
                long valueAddress = location.getValueAddress().getBaseOffset();
                unsafe.putDouble(valueAddress, rank);
            }
        }

        public void putNewKeyValue(long keyAddress, double rank) {
            Location location = bytesToBytesMap.lookup(null, keyAddress, 8);
            if (!location.isDefined()) {
                unsafe.putDouble(bufValue, rank);
                location.putNewKey(null, keyAddress, 8, null, bufValue, 8);
            } else {
                long valueAddress = location.getValueAddress().getBaseOffset();
                double value = unsafe.getDouble(valueAddress);
                unsafe.putDouble(valueAddress, rank + value);
            }
        }

        public void free() {
            bytesToBytesMap.free();
        }

        public String toString() {
            Iterator<Location> iterator = bytesToBytesMap.iterator();
            StringBuilder stringBuilder = new StringBuilder();
            while (iterator.hasNext()) {
                Location location = iterator.next();
                long key = unsafe.getLong(location.getKeyAddress().getBaseOffset());
                double value = unsafe.getDouble(location.getValueAddress().getBaseOffset());
                stringBuilder.append(key + ": " + value + "\n");
            }
            return stringBuilder.toString();
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
            unsafe.putLong(offset + address, value);
        }

        public UnsafeMap getRanks() {
            UnsafeMap ranks = new UnsafeMap(keyCount);
            long offset = address;
            while (offset < size + address) {
                ranks.putDouble(offset, 1.0);
                offset += 8;
                long length = unsafe.getLong(offset);
                offset += 8;
                offset += 8 * length;
            }
            return ranks;
        }

        public UnsafeMap getJoin(UnsafeMap ranks) {
            UnsafeMap joinMap = new UnsafeMap(idCount);
            long offset = address;
            while (offset < size + address) {
                joinMap.putPairList(offset, offset + 8);
                offset += 8;
                long length = unsafe.getLong(offset);
                offset += 8 * (length + 1);
            }
            Iterator<Location> keyValues = ranks.bytesToBytesMap.iterator();
            while (keyValues.hasNext()) {
                Location keyValue = keyValues.next();
                long keyAddress = keyValue.getKeyAddress().getBaseOffset();
                double value = unsafe.getDouble(keyValue.getValueAddress().getBaseOffset());
                joinMap.putPairRank(keyAddress, value * 0.85 + 0.15);
            }
            ranks.free();
            return joinMap;
        }

        public UnsafeMap getReduce(UnsafeMap joinMap) {
            Iterator<Location> keyValues = joinMap.bytesToBytesMap.iterator();
            UnsafeMap reduceMap = new UnsafeMap(idCount);
            while (keyValues.hasNext()) {
                Location keyValue = keyValues.next();
                long tmpOffset = keyValue.getValueAddress().getBaseOffset();
                double rankValue = unsafe.getDouble(tmpOffset);
                if (rankValue < 0)
                    continue;
                long listAddress = unsafe.getLong(tmpOffset + 8);
                if (listAddress < 0)
                    continue;
                long length = unsafe.getLong(listAddress);
                double newValue = rankValue / length;
                long offset = listAddress + 8;
                for (int i = 1; i <= length; i++) {
                    reduceMap.putNewKeyValue(offset, newValue);
                    offset += 8;
                }
            }
            joinMap.free();
            return reduceMap;
        }

    }

    Chunk cacheChunk;

    @Override
    protected void cache(Map<Integer, ArrayList<Integer>> links) {
        long startTime = System.currentTimeMillis();
        cacheChunk = new Chunk(count * 8 + keyCount * 8);
        int offset = 0;
        for (Map.Entry<Integer, ArrayList<Integer>> entry : links.entrySet()) {
            int key = entry.getKey();
            cacheChunk.putValue(key, offset);
            offset += 8;
            ArrayList<Integer> values = entry.getValue();
            cacheChunk.putValue(values.size(), offset);
            offset += 8;
            for (int i = 0; i < values.size(); i++) {
                cacheChunk.putValue(values.get(i), offset);
                offset += 8;
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Deca cache bytes time: " + (endTime - startTime) + "ms");
    }

    @Override
    public void compute(int iterations) {
        UnsafeMap ranks = cacheChunk.getRanks();
        for (int i = 0; i < iterations; i++) {
            UnsafeMap joinMap = cacheChunk.getJoin(ranks);
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
