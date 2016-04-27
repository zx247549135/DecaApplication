package microBenchmark;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.*;

/**
 * Created by zx on 16-4-11.
 */
public class MultiThreadDecaJavaLR extends LR{

    private int partitions;
    private int cores;

    MultiThreadDecaJavaLR(int partitions, int cores){
        this.partitions = partitions;
        this.cores = cores;
    }

    private double[] w;

    class Chunk{
        int dimensions;
        int size;
        Unsafe unsafe;
        long address;
        long count = 0;

        Chunk(int dimensions, int size){
            this.size = size;
            this.dimensions = dimensions;
            try {
                Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                unsafe = (Unsafe) unsafeField.get(null);
                address = unsafe.allocateMemory(size);
            }catch(Exception e){
                System.out.println("init error: " + e);
            }
        }

        public void putValue(double value, long offset){
            unsafe.putDouble(offset + address, value);
            count += 8;
        }

        public double[] getValue(){
            double[] gradient = new double[dimensions];
            long offset = address;
            double y;
            while(offset < count + address){
                y = unsafe.getDouble(offset);
                offset += 8;
                long current = offset;
                double dot = 0.0;
                for(int j = 0; j < dimensions; j ++){
                    dot += w[j] * unsafe.getDouble(current);
                    current += 8;
                }
                double tmp = (1 / (1 + Math.exp(-y * dot)) - 1) * y;
                for(int j = 0; j < dimensions; j ++){
                    gradient[j] += tmp * unsafe.getDouble(offset);
                    offset += 8;
                }
            }
            return gradient;
        }
    }

    class RunThread implements Callable {
        int partitionId;

        RunThread(int partitionId){
            this.partitionId = partitionId;
        }

        @Override
        public Object call() {
            return cacheBytes[partitionId].getValue();
        }

    }

    private Chunk[] cacheBytes;
    ExecutorService executor;

    public void textFile(int dimensions, int nums) {
        this.D = dimensions;
        this.N = nums;

        w = new double[D];
        for (int i = 0; i < D; i++) {
            w[i] = 2 * random.nextDouble() - 1;
        }

        try {
            int offset = 0;
            cacheBytes = new Chunk[partitions];
            for(int i = 0; i < partitions; i ++){
                cacheBytes[i] = new Chunk(D, N * (D+1) * 8 / partitions);
            }

            int partitionId = 0;
            for (int i = 0; i < N; i++) {
                int y;
                if (i % 2 == 0)
                    y = 0;
                else
                    y = 1;

                if(partitionId >= partitions) {
                    partitionId -= partitions;
                    offset += (D + 1) * 8;
                }
                cacheBytes[partitionId].putValue(y, offset);
                offset += 8;
                for (int j = 0; j < D; j++) {
                    double tmp = random.nextGaussian() + y * R;
                    cacheBytes[partitionId].putValue(tmp, offset);
                    offset += 8;
                }
                partitionId += 1;
                offset -= (D + 1) * 8;
            }
        }catch(Exception e){
            System.out.println("textFile error: " + e);
        }
    }

    public void compute(int iterations){
        executor = Executors.newFixedThreadPool(cores);
        for(int iter = 0; iter < iterations; iter ++) {
            Future[] futures = new Future[partitions];
            try {
                for(int i = 0; i < partitions; i ++){
                    Callable callable = new RunThread(i);
                    futures[i] = executor.submit(callable);
                }
                double[] gradient = new double[D];
                for (int i = 0; i < partitions; i++) {
                    double[] subgradient = (double[]) futures[i].get(100, TimeUnit.MINUTES);
                    for (int j = 0; j < D; j++) {
                        gradient[j] += subgradient[j];
                    }
                }
                for (int j = 0; j < D; j++) {
                    w[j] -= gradient[j];
                }
            }catch (Exception e){
                System.out.println("compute error: " + e);
            }
        }
//        System.out.print("Final w: ");
//        System.out.println(Arrays.toString(w));
    }

    public void shutdown(){
        executor.shutdown();
    }
}
