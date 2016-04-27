package microBenchmark;

import java.util.concurrent.TimeUnit;

/**
 * Created by zx on 16-4-7.
 */
public class LRHelper {

    public static void main(String[] args) {

        int dimensions = Integer.parseInt(args[1]);
        int nums = Integer.parseInt(args[2]);
        int iterations = Integer.parseInt(args[3]);

        int partitions = Integer.parseInt(args[4]);
        int cores = Integer.parseInt(args[5]);

        int type = Integer.parseInt(args[0]);
        String lrName = "";

        LR lr = null;
        switch (type) {
            case 1: {
                lr = new JavaLR();
                lrName = "JavaLR";
                break;
            }
            case 2: {
                lr = new DecaJavaLR();
                lrName = "DecaJavaLR";
                break;
            }
            case 3: {
                lr = new SerializeJavaLR();
                lrName = "SerializeJavaLR";
                break;
            }
            case 4: {
                lr = new MultiThreadJavaLR(partitions, cores);
                lrName = "MultiThreadJavaLR";
                break;
            }
            case 5: {
                lr = new MultiThreadDecaJavaLR(partitions, cores);
                lrName = "MultiThreadDecaJavaLR";
                break;
            }
            case 6: {
                lr = new MultiThreadSerializeJavaLR(partitions, cores);
                lrName = "MultiThreadSerialzieJavaLR";
                break;
            }
        }
        long startTime = System.currentTimeMillis();
        lr.textFile(dimensions, nums);
        long endTime = System.currentTimeMillis();
        System.out.println(lrName + " textFile time: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        lr.compute(20);
        endTime = System.currentTimeMillis();
        System.out.println(lrName + " warm time: " + (endTime - startTime) + "ms");
        lr.shutdown();
        triggerGC();

        startTime = System.currentTimeMillis();
        lr.compute(iterations);
        endTime = System.currentTimeMillis();
        System.out.println(lrName + " compute time: " + (endTime - startTime) + "ms");
        lr.shutdown();
    }

    private static void triggerGC() {
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
