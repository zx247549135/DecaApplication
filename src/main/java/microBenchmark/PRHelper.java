package microBenchmark;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Created by zx on 16-4-6.
 */
public class PRHelper {

    public static void main(String[] args) {
        int type = Integer.parseInt(args[0]);
        File dataFile = new File(args[1]);
        int iterations = Integer.parseInt(args[2]);

        PR pr = null;

        switch (type) {
            case 1: {
                pr = new JavaPR();
                break;
            }
            case 2: {
                pr = new DecaNewPR();
                break;
            }
            case 3: {
                pr = new SerializeJavaPR();
                break;
            }
            case 4: {
                pr = new DecaJavaPR();
                break;
            }
            case 5: {
                pr = new SparkJavaPR();
                break;
            }
            case 6: {
                int numCores = Integer.parseInt(args[3]);
                int numPartitions = Integer.parseInt(args[4]);
                pr = new MultiThreadJavaPR(numCores, numPartitions);
                break;
            } case 7: {
                int numCores = Integer.parseInt(args[3]);
                int numPartitions = Integer.parseInt(args[4]);
                pr = new MultiThreadSerializeJavaPR(numCores, numPartitions);
                break;
            }
        }

        long startTime = System.currentTimeMillis();
        pr.textFile(dataFile);
        long endTime = System.currentTimeMillis();
        System.out.println(pr.name + " textFile time: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        pr.compute(5);
        endTime = System.currentTimeMillis();
        System.out.println(pr.name + " warm-up time: " + (endTime - startTime) + "ms");

        triggerGC();

        startTime = System.currentTimeMillis();
        pr.compute(iterations);
        endTime = System.currentTimeMillis();
        System.out.println(pr.name + " compute time: " + (endTime - startTime) + "ms");

        pr.close();
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
