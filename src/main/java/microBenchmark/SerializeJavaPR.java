package microBenchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.chill.AllScalaRegistrar;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zx on 16-4-7.
 */
public class SerializeJavaPR extends PR {

    public SerializeJavaPR() {
        name = "SerializeJavaPR";
    }

    Chunk cacheSerializeBytes;
    Kryo kryo = new Kryo();

    @Override
    protected void cache(Map<Integer, ArrayList<Integer>> links) {
        cacheSerializeBytes = new Chunk(count * 4 + keyCount * 4 * 6);
        new AllScalaRegistrar().apply(kryo);
//        ObjectOutputStream obj = null;
//        try {
//            obj = new ObjectOutputStream(cacheSerializeBytes);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        Output output = new Output(obj);
        long startTime = System.currentTimeMillis();
        Output output = new Output(cacheSerializeBytes);
        for (Map.Entry<Integer, ArrayList<Integer>> entry : links.entrySet()) {
            int key = entry.getKey();
            ArrayList<Integer> value = entry.getValue();
            Tuple2<Integer, ArrayList<Integer>> keyValue = new Tuple2<>(key, value);
            kryo.writeObject(output, keyValue);
        }
        output.flush();
        output.close();
        long endTime = System.currentTimeMillis();
        System.out.println("Serializer cache bytes time: " + (endTime - startTime) + "ms");
    }

    @Override
    public void compute(int iterations) {
//        try {
            HashMap<Integer, Double> ranks = new HashMap<>((int) (keyCount / 0.75f) + 1);
//            ObjectInputStream obj1 = new ObjectInputStream(cacheSerializeBytes.toInputStream());
//            Input input1 = new Input(obj1);
            Input input1 = new Input(cacheSerializeBytes.toInputStream());
            for (int i = 0; i < keyCount; i++) {
                Tuple2<Integer, ArrayList<Integer>> keyValue =
                        (Tuple2<Integer, ArrayList<Integer>>) kryo.readObject(input1, Tuple2.class);
                ranks.put(keyValue._1(), 1.0);
            }
            for (int i = 0; i < iterations; i++) {
//                ObjectInputStream obj2 = new ObjectInputStream(cacheSerializeBytes.toInputStream());
//                Input input2 = new Input(obj2);
                Input input2 = new Input(cacheSerializeBytes.toInputStream());
                HashMap<Integer, Pair> joinHashMap = new HashMap<>((int) (idCount / 0.75f) + 1);
                for (int j = 0; j < keyCount; j++) {
                    Tuple2<Integer, ArrayList<Integer>> keyValue =
                            (Tuple2<Integer, ArrayList<Integer>>) kryo.readObject(input2, Tuple2.class);
                    int key = keyValue._1();
                    ArrayList<Integer> urls = keyValue._2();
                    Pair tmpJoin = new Pair(null, urls);
                    joinHashMap.put(key, tmpJoin);
                }
                for (Map.Entry<Integer, Double> keyValue : ranks.entrySet()) {
                    if (joinHashMap.containsKey(keyValue.getKey())) {
                        joinHashMap.get(keyValue.getKey()).set1(keyValue.getValue() * 0.85 + 0.15);
                    } else {
                        joinHashMap.put(keyValue.getKey(), new Pair(keyValue.getValue() * 0.85 + 0.15, null));
                    }
                }
                HashMap<Integer, Double> reduceHashMap = new HashMap<>((int) (idCount / 0.75f) + 1);
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
//        } catch (IOException e) {
//            System.out.println("compute error: " + e);
//        }
    }

}
