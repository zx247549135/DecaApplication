package microBenchmark;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zx on 16-4-6.
 */
public class JavaPR extends PR {

    public JavaPR() {
        name = "JavaPR";
    }

    Tuple2<Integer, ArrayList<Integer>>[] arrayLinks;

    @Override
    protected void cache(Map<Integer, ArrayList<Integer>> links) {
        arrayLinks = new Tuple2[links.size()];
        int index = 0;
        for (Map.Entry<Integer, ArrayList<Integer>> entry : links.entrySet()) {
            arrayLinks[index++] = new Tuple2<>(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void compute(int iterations) {
        HashMap<Integer, Double> ranks = new HashMap<>((int) (keyCount / 0.75f) + 1);
        for (int i = 0; i < arrayLinks.length; i++)
            ranks.put(arrayLinks[i]._1(), 1.0);
        for (int i = 0; i < iterations; i++) {
            HashMap<Integer, Pair> joinHashMap = new HashMap<>((int) (idCount / 0.75f) + 1);
            for (int j = 0; j < arrayLinks.length; j++) {
                int key = arrayLinks[j]._1();
                ArrayList<Integer> urls = arrayLinks[j]._2();
                Pair tmpJoin = new Pair(null, urls);
                joinHashMap.put(key, tmpJoin);
            }
            for (Map.Entry<Integer, Double> keyValue : ranks.entrySet()) {
                Integer key = keyValue.getKey();
                if (joinHashMap.containsKey(key)) {
                    joinHashMap.get(key).set1(keyValue.getValue() * 0.85 + 0.15);
                } else {
                    joinHashMap.put(key, new Pair(keyValue.getValue() * 0.85 + 0.15, null));
                }
            }
            HashMap<Integer, Double> reduceHashMap = new HashMap<>((int) (idCount / 0.75f));
            for (Map.Entry<Integer, Pair> joinKeyValue : joinHashMap.entrySet()) {
                Pair rankValue = joinKeyValue.getValue();
                if (rankValue._1 != null && rankValue._2 != null) {
                    ArrayList<Integer> urls = rankValue._2;
                    double newValue = rankValue._1 / urls.size();
                    for (Integer url : urls) {
                        if (reduceHashMap.containsKey(url)) {
                            double value = reduceHashMap.get(url);
                            reduceHashMap.put(url, value + newValue);
                        } else {
                            reduceHashMap.put(url, newValue);
                        }
                    }
                }
            }
            ranks = reduceHashMap;
        }
        //System.out.println(ranks);
    }

}
