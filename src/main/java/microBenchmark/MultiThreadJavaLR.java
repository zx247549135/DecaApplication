package microBenchmark;

import java.util.concurrent.*;

/**
 * Created by zx on 16-4-10.
 */
public class MultiThreadJavaLR extends LR {

    private int partitions = 8;
    private int cores = 2;
    ExecutorService executor;

    MultiThreadJavaLR(int paritions, int cores){
        this.partitions = paritions;
        this.cores = cores;
    }

    class RunThread implements Callable{
        int partitionId;

        RunThread(int partitionId){
            this.partitionId = partitionId;
        }

        @Override
        public Object call() {
            double[] gradient = new double[D];
            DataPoint[] block = cache[partitionId];
            for (int i = 0; i < block.length; i++) {
                double[] subgradient = new double[D];
                DataPoint p = block[i];
                if (p == null)
                    break;
                double dot = dot(w, p.x);
                double tmp = (1 / (1 + Math.exp(-p.y * dot)) - 1) * p.y;
                for (int j = 0; j < D; j++) {
                    gradient[j] += tmp * p.x[j];
                }
            }
            return gradient;
        }

        public double dot(double[] a, double[] b) {
            double x = 0;
            for (int i = 0; i < D; i++) {
                x += a[i] * b[i];
            }
            return x;
        }
    }

    private DataPoint[][] cache;
    private static double[] w;

    public void textFile(int dimension, int nums) {
        this.D = dimension;
        this.N = nums;

        w = new double[D];
        for (int i = 0; i < D; i++) {
            w[i] = 2 * random.nextDouble() - 1;
        }
//        System.out.print("Initial w: ");
//        System.out.println(Arrays.toString(w));

        cache = new DataPoint[partitions][N / partitions + 1];
        int partitionId = 0;
        int cacheIndex = 0;
        for (int i = 0; i < N; i++) {
            int y;
            if (i % 2 == 0)
                y = 0;
            else
                y = 1;
            double[] x = new double[D];
            for (int j = 0; j < D; j++) {
                x[j] =  random.nextGaussian() + y * R;
            }

            if(partitionId >= partitions) {
                partitionId -= partitions;
                cacheIndex += 1;
            }
            cache[partitionId][cacheIndex] = new DataPoint(x, y);
            partitionId += 1;
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
            } catch (Exception e) {
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