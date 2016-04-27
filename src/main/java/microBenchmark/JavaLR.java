package microBenchmark;

/**
 * Created by zx on 16-4-6.
 */

import java.util.Arrays;

public class JavaLR extends LR {

    private DataPoint[] cache;
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

        cache = new DataPoint[N];
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

            cache[i] = new DataPoint(x, y);
        }
    }

    public void compute(int iterations){
        for(int iter = 0; iter < iterations; iter ++) {
            double[] gradient = new double[D];
            for (int i = 0; i < N; i++) {
                DataPoint p = cache[i];
                double dot = dot(w, p.x);
                double tmp = (1 / (1 + Math.exp(-p.y * dot)) - 1) * p.y;
                for (int j = 0; j < D; j++) {
                    gradient[j] += tmp * p.x[j];
                }
            }
            for (int j = 0; j < D; j++) {
                w[j] -= gradient[j];
            }
        }
        System.out.print("Final w: ");
        System.out.println(Arrays.toString(w));
    }

}
