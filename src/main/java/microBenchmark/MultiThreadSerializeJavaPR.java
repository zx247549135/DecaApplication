package microBenchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.chill.AllScalaRegistrar;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by zx on 16-4-6.
 */
public class MultiThreadSerializeJavaPR extends MultiThreadPR {

    public MultiThreadSerializeJavaPR(int numCores, int numPartitions) {
        super(numCores, numPartitions);
        name = "MultiThreadSerializeJavaPR";
    }

    Kryo[] kryoes;
    Chunk[] blocks;

    @Override
    protected void cache(Map<Integer, ArrayList<Integer>> links) {
        super.cache(links);

        kryoes = new Kryo[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            Kryo kryo = new Kryo();
            new AllScalaRegistrar().apply(kryo);
            kryoes[i] = kryo;
        }

        blocks = new Chunk[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            blocks[i] = new Chunk(initCounts[i] * 4 + initKeyCounts[i] * 4 * 6);
        }
        Output[] outputs = new Output[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            outputs[i] = new Output(blocks[i]);
        }

        long startTime = System.currentTimeMillis();
        for (Map.Entry<Integer, ArrayList<Integer>> entry : links.entrySet()) {
            int key = entry.getKey();
            ArrayList<Integer> value = entry.getValue();
            int partitionId = key % numPartitions;
            kryoes[partitionId].writeObject(outputs[partitionId], new Tuple2<>(key, value));
        }
        for (int i = 0; i < numPartitions; i++) {
            outputs[i].flush();
            outputs[i].close();
        }
//        for (int i = 0; i < numPartitions; i++) {
//            System.out.println(blocks[i].capacity());
//        }
        long endTime = System.currentTimeMillis();
        System.out.println("Serializer cache bytes time: " + (endTime - startTime) + "ms");
    }

    private class InitTask implements Callable<HashMap<Integer, Double>[]> {
        int partitionId;

        InitTask(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public HashMap<Integer, Double>[] call() throws Exception {
            Kryo kryo = kryoes[partitionId];
            Input input = new Input(blocks[partitionId].toInputStream());
            int[] counts = mapOutKeyCounts[partitionId];
            HashMap<Integer, Double>[] result = new HashMap[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
                result[i] = new HashMap<>(counts[i]);
            }

            int size = initKeyCounts[partitionId];
            for (int i = 0; i < size; i++) {
                Tuple2<Integer, ArrayList<Integer>> keyValue =
                    (Tuple2<Integer, ArrayList<Integer>>) kryo.readObject(input, Tuple2.class);
                ArrayList<Integer> urls = keyValue._2();
                final double value = 1.0 / urls.size();
                for (Integer url : urls) {
                    double newValue = value;
                    HashMap<Integer, Double> outMap = result[url % numPartitions];
                    if (outMap.containsKey(url)) {
                        newValue += outMap.get(url);
                    }
                    outMap.put(url, newValue);
                }
            }

            return result;
        }
    }

    private class IterTask implements Callable<HashMap<Integer, Double>[]> {
        int partitionId;
        HashMap<Integer, Double>[] inMessages;

        IterTask(int partitionId, HashMap<Integer, Double>[] inMessages) {
            this.partitionId = partitionId;
            this.inMessages = inMessages;
        }


        @Override
        public HashMap<Integer, Double>[] call() throws Exception {
            Kryo kryo = kryoes[partitionId];
            Input input = new Input(blocks[partitionId].toInputStream());
            int count = reduceInKeyCounts[partitionId];

            // reduceBykey:reduce-side
            HashMap<Integer, Double> reduceMap = new HashMap<>(count);
            for (Map<Integer, Double> map : inMessages) {
                for (Map.Entry<Integer, Double> entry : map.entrySet()) {
                    Integer key = entry.getKey();
                    double value = entry.getValue();
                    if (reduceMap.containsKey(key)) {
                        value += reduceMap.get(key);
                    }
                    reduceMap.put(key, value);
                }
            }

            // join
            HashMap<Integer, Pair> joinHashMap = new HashMap<>(count);
            int size = initKeyCounts[partitionId];
            for (int i = 0; i < size; i++) {
                Tuple2<Integer, ArrayList<Integer>> keyValue =
                    (Tuple2<Integer, ArrayList<Integer>>) kryo.readObject(input, Tuple2.class);
                int key = keyValue._1();
                ArrayList<Integer> urls = keyValue._2();
                Pair tmpJoin = new Pair(null, urls);
                joinHashMap.put(key, tmpJoin);
            }
            for (Map.Entry<Integer, Double> keyValue : reduceMap.entrySet()) {
                Integer key = keyValue.getKey();
                if (joinHashMap.containsKey(key)) {
                    joinHashMap.get(key).set1(keyValue.getValue() * 0.85 + 0.15);
                } else {
                    joinHashMap.put(key, new Pair(keyValue.getValue() * 0.85 + 0.15, null));
                }
            }

            // reduceBykey:map-side
            int[] counts = mapOutKeyCounts[partitionId];
            HashMap<Integer, Double>[] result = new HashMap[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
                result[i] = new HashMap<>(counts[i]);
            }
            for (Map.Entry<Integer, Pair> joinKeyValue : joinHashMap.entrySet()) {
                Pair rankValue = joinKeyValue.getValue();
                if (rankValue._1 != null && rankValue._2 != null) {
                    ArrayList<Integer> urls = rankValue._2;
                    final double value = rankValue._1 / urls.size();
                    for (Integer url : urls) {
                        double newValue = value;
                        HashMap<Integer, Double> outMap = result[url % numPartitions];
                        if (outMap.containsKey(url)) {
                            newValue += outMap.get(url);
                        }
                        outMap.put(url, newValue);
                    }
                }
            }

            return result;
        }
    }

    private class EndTask implements Callable<HashMap<Integer, Double>> {
        int partitionId;
        HashMap<Integer, Double>[] inMessages;

        EndTask(int partitionId, HashMap<Integer, Double>[] inMessages) {
            this.partitionId = partitionId;
            this.inMessages = inMessages;
        }

        @Override
        public HashMap<Integer, Double> call() throws Exception {
            // reduceBykey:reduce-side
            HashMap<Integer, Double> reduceMap = new HashMap<>(count);
            for (Map<Integer, Double> map : inMessages) {
                for (Map.Entry<Integer, Double> entry : map.entrySet()) {
                    Integer key = entry.getKey();
                    double value = entry.getValue();
                    if (reduceMap.containsKey(key)) {
                        value += reduceMap.get(key);
                    }
                    reduceMap.put(key, value);
                }
            }
            return reduceMap;
        }
    }

    @Override
    public void compute(int iterations) {
        HashMap<Integer, Double>[][] outMessages = new HashMap[numPartitions][];
        Future<HashMap<Integer, Double>[]>[] futures = new Future[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            futures[i] = executor.submit(new InitTask(i));
        }
        for (int i = 0; i < numPartitions; i++) {
            try {
                outMessages[i] = futures[i].get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        HashMap<Integer, Double>[][] inMessages = new HashMap[numPartitions][numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numPartitions; j++) {
                inMessages[j][i] = outMessages[i][j];
            }
        }

        for (int iter = 1; iter < iterations; iter++) {
            for (int i = 0; i < numPartitions; i++) {
                futures[i] = executor.submit(new IterTask(i, inMessages[i]));
            }
            for (int i = 0; i < numPartitions; i++) {
                try {
                    outMessages[i] = futures[i].get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (int i = 0; i < numPartitions; i++) {
                for (int j = 0; j < numPartitions; j++) {
                    inMessages[j][i] = outMessages[i][j];
                }
            }
        }

        Future<HashMap<Integer, Double>>[] resultFutures = new Future[numPartitions];
        HashMap<Integer, Double>[] results = new HashMap[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            resultFutures[i] = executor.submit(new EndTask(i, inMessages[i]));
        }
        for (int i = 0; i < numPartitions; i++) {
            try {
                results[i] = resultFutures[i].get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < numPartitions; i++) {
            System.out.println(results[i]);
        }
    }

}
