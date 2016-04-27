package microBenchmark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by zx on 16-4-22.
 */
public abstract class LR {
    public static int D = 10;   // Number of dimensions
    public int N = 1000;  // Number of data points
    public final double R = 0.00007;  // Scaling factor

    Random random = new Random(42);

    static class DataPoint implements Serializable {

        DataPoint(){}

        DataPoint(double[] x, double y) {
            this.x = x;
            this.y = y;
        }

        double[] x;
        double y;

        public String toString(){
            return "DataPoint:(" + y + Arrays.toString(x) + ")";
        }
    }

    public abstract void textFile(int dimension, int nums);

    public abstract void compute(int iterations);

    public void shutdown(){}

    public double dot(double[] a, double[] b) {
        double x = 0;
        for (int i = 0; i < D; i++) {
            x += a[i] * b[i];
        }
        return x;
    }

}
