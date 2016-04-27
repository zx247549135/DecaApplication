package microBenchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.util.concurrent.*;

/**
 * Created by zx on 16-4-11.
 */
public class MultiThreadSerializeJavaLR extends LR {

    private int partitions;
    private int cores;

    MultiThreadSerializeJavaLR(int partitions, int cores){
        this.partitions = partitions;
        this.cores = cores;
    }

    private double[] w;

    class Chunk extends ByteArrayOutputStream {
        int objectCount = 0;
        int size = 0;

        Chunk(int size){
            super(size);
            this.size = size;
        }

        public void increase(int num){
            objectCount += num;
        }

        public ByteArrayInputStream toInputStream(){
            return new ByteArrayInputStream(buf);
        }
    }

    class RunThread implements Callable {
        int partitionId = 0;

        RunThread(int partitionId){
            this.partitionId = partitionId;
        }

        @Override
        public Object call() {
            double[] gradient = new double[D];
            Chunk block = cacheSerializeBytes[partitionId];
            try {
                Input input = new Input(block.toInputStream());
                for (int i = 0; i < block.objectCount; i++) {
                    double[] subgradient = new double[D];
                    DataPoint p = (DataPoint) kryos[partitionId].readObject(input, registrations[partitionId].getType());
                    double dot = dot(w, p.x);
                    double tmp = (1 / (1 + Math.exp(-p.y * dot)) - 1) * p.y;
                    for (int j = 0; j < D; j++) {
                        gradient[j] += tmp * p.x[j];
                    }
                }
                input.close();
            }catch (Exception e){
                System.out.println("Thread error: " + partitionId + ";" + e);
                //e.printStackTrace();
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

    private Chunk[] cacheSerializeBytes;
    Kryo[] kryos;
    Registration[] registrations;
    ExecutorService executor;

    public void textFile(int dimension, int nums) {
        this.D = dimension;
        this.N = nums;

        kryos = new Kryo[partitions];
        registrations = new Registration[partitions];

        w = new double[D];
        for (int i = 0; i < D; i++) {
            w[i] = 2 * random.nextDouble() - 1;
        }

        cacheSerializeBytes = new Chunk[partitions];
        int chunkSize = N / partitions * (D + 1) * 8 + N / partitions * 3;
        for (int i = 0; i < partitions; i++) {
            cacheSerializeBytes[i] = new Chunk(chunkSize);
        }
        Output[] outputs = new Output[partitions];
        for (int i = 0; i < partitions; i++) {
            outputs[i] = new Output(cacheSerializeBytes[i]);
        }
        for (int i = 0; i < partitions; i ++) {
            kryos[i] = new Kryo();
            registrations[i] = kryos[i].register(DataPoint.class);
        }
        int partitionId = 0;
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

            if (partitionId >= partitions) {
                partitionId -= partitions;
            }
            DataPoint tmp = new DataPoint(x, y);
            kryos[partitionId].writeObject(outputs[partitionId], tmp);
            cacheSerializeBytes[partitionId].increase(1);
            partitionId += 1;
        }
        for (int i = 0; i < partitions; i++) {
            outputs[i].flush();
            outputs[i].close();
        }
    }

    public void compute(int iterations) {
        executor = Executors.newFixedThreadPool(cores);
        for (int iter = 0; iter < iterations; iter++) {
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
