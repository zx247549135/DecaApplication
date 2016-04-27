package microBenchmark;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by zx on 16-4-6.
 */
public class MultiThreadJavaPR extends MultiThreadPR {

    public MultiThreadJavaPR(int numCores, int numPartitions) {
        super(numCores, numPartitions);
        name = "MultiThreadJavaPR";
    }

    Tuple2<Integer, ArrayList<Integer>>[][] blocks;

    @Override
    protected void cache(Map<Integer, ArrayList<Integer>> links) {
        super.cache(links);

        blocks = new Tuple2[numPartitions][];
        for (int i = 0; i < numPartitions; i++) {
            blocks[i] = new Tuple2[initKeyCounts[i]];
        }
        int[] blockIndices = new int[numPartitions];
        for (Map.Entry<Integer, ArrayList<Integer>> entry : links.entrySet()) {
            int key = entry.getKey();
            ArrayList<Integer> value = entry.getValue();
            int partitionId = key % numPartitions;
            blocks[partitionId][blockIndices[partitionId]++] = new Tuple2<>(key, value);
        }
    }

    private class InitTask implements Callable<HashMap<Integer, Double>[]> {
        int partitionId;

        InitTask(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public HashMap<Integer, Double>[] call() throws Exception {
            Tuple2<Integer, ArrayList<Integer>>[] block = blocks[partitionId];
            int[] counts = mapOutKeyCounts[partitionId];
            HashMap<Integer, Double>[] result = new HashMap[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
                result[i] = new HashMap<>(counts[i]);
            }

            for (int i = 0; i < block.length; i++) {
                if (block[i] == null) break;
                ArrayList<Integer> urls = block[i]._2();
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
            Tuple2<Integer, ArrayList<Integer>>[] block = blocks[partitionId];
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
            for (int i = 0; i < block.length; i++) {
                if (block[i] == null) break;
                int key = block[i]._1();
                ArrayList<Integer> urls = block[i]._2();
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

//        for (int i = 0; i < numPartitions; i++) {
//            System.out.println(results[i]);
//        }
    }

}
