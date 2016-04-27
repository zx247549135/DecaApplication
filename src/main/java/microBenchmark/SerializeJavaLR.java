package microBenchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.util.Arrays;

/**
 * Created by zx on 16-4-6.
 */
public class SerializeJavaLR extends LR {

    private double[] w;

    class Chunk extends ByteArrayOutputStream{
        int size;

        public int capacity() {
            return buf.length;
        }

        Chunk(int size){
            super(size);
            this.size = size;
        }

        public ByteArrayInputStream toInputStream(){
            return new ByteArrayInputStream(buf);
        }
    }

    static class DataPoint implements Serializable {

        DataPoint(){}

        DataPoint(double[] x, double y) {
            this.x = x;
            this.y = y;
        }

        double[] x;
        double y;
    }

    private Chunk cacheSerializeBytes;
    Kryo kryo = new Kryo();
    Registration registration;

    public void textFile(int dimension, int nums){
        this.D = dimension;
        this.N = nums;

        w = new double[D];
        for (int i = 0; i < D; i++) {
            w[i] = 2 * random.nextDouble() - 1;
        }
//        System.out.print("Initial w: ");
//        System.out.println(Arrays.toString(w));

//        try {
            cacheSerializeBytes = new Chunk((D + 1 + 1) * N * 8);
//            ObjectOutputStream obj = new ObjectOutputStream(cacheSerializeBytes);
//            Output output = new Output(obj);
            Output output = new Output(cacheSerializeBytes);
            registration = kryo.register(DataPoint.class);
            for (int i = 0; i < N; i++) {
                int y;
                if (i % 2 == 0)
                    y = 0;
                else
                    y = 1;
                double[] x = new double[D];
                for (int j = 0; j < D; j++) {
                    x[j] = random.nextGaussian() + y * R;
                }
                DataPoint tmp = new DataPoint(x, y);
                kryo.writeObject(output, tmp);
            }
            output.flush();
            output.close();
//        }catch(IOException e){
//            System.out.println("textFile error: " + e);
//        }
    }

    public void compute(int iterations){
//        try {
            for (int iter = 0; iter < iterations; iter++) {
//                ObjectInputStream obj = new ObjectInputStream(cacheSerializeBytes.toInputStream());
//                Input input = new Input(obj);
                Input input = new Input(cacheSerializeBytes.toInputStream());
                double[] gradient = new double[D];
                for (int i = 0; i < N; i++) {
                    double[] gradient1 = new double[D];
                    DataPoint p = (DataPoint) kryo.readObject(input, registration.getType());
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
//        }catch(IOException e){
//            System.out.println("compute error: " + e);
//        }
    }

    public double dot(double[] a, double[] b) {
        double x = 0;
        for (int i = 0; i < D; i++) {
            x += a[i] * b[i];
        }
        return x;
    }
}
