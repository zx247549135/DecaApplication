package microBenchmark;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by zx on 16-4-6.
 */

public class DecaJavaLR extends LR {

    private double[] w;

    class Chunk{
        int dimensions;
        int size;
        Unsafe unsafe;
        long address;

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
        }

        public double[] getValue(){
            double[] gradient = new double[dimensions];
            long offset = address;
            double y;
            while(offset < size + address){
                double[] gradient1 = new double[D];
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

    private Chunk cacheBytes;

    public void textFile(int dimensions, int nums) {
        this.D = dimensions;
        this.N = nums;

        w = new double[D];
        for (int i = 0; i < D; i++) {
            w[i] = 2 * i * 0.037 - 1;
        }
//        System.out.print("Initial w: ");
//        System.out.println(Arrays.toString(w));

        try {
            int offset = 0;
            cacheBytes = new Chunk(D, N * (D+1) * 8);
            for (int i = 0; i < N; i++) {
                int y;
                if (i % 2 == 0)
                    y = 0;
                else
                    y = 1;
                cacheBytes.putValue(y, offset);
                offset += 8;
                for (int j = 0; j < D; j++) {
                    double tmp = i * 0.000013 + j * 0.00079 + y * R;
                    cacheBytes.putValue(tmp, offset);
                    offset += 8;
                }
            }
        }catch(Exception e){
            System.out.println("textFile error: " + e);
        }
    }

    public void compute(int iterations){
        for(int iter = 0; iter < iterations; iter ++) {
            double[] gradient = cacheBytes.getValue();
            for (int j = 0; j < D; j++) {
                w[j] -= gradient[j];
            }
        }
//        System.out.print("Final w: ");
//        System.out.println(Arrays.toString(w));
    }

}
