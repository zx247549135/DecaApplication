package microBenchmark;

import org.apache.spark.util.collection.AppendOnlyMap;
import scala.Tuple2;
import scala.runtime.AbstractFunction2;

import java.util.ArrayList;

/**
 * Created by zx on 16-4-6.
 */
public class SparkJavaPR extends JavaPR {

    public SparkJavaPR() {
        name = "SparkJavaPR";
    }

    static final class UpdateFunc2 extends AbstractFunction2<Object, Double, Double> {
        public double adder;

        @Override
        public Double apply(Object hasOld, Double old) {
            if (!((Boolean) hasOld)) {
                return adder;
            }
            return old + adder;
        }
    }

    final UpdateFunc2 updateFunc2 = new UpdateFunc2();

    static final class UpdateFunc3 extends AbstractFunction2<Object, Pair, Pair> {
        public double rank;

        @Override
        public Pair apply(Object hasOld, Pair old) {
            if (!((Boolean) hasOld)) {
                return new Pair(rank, null);
            }
            old.set1(rank);
            return old;
        }
    }

    final UpdateFunc3 updateFunc3 = new UpdateFunc3();

    @Override
    public void compute(int iterations) {
        AppendOnlyMap<Integer, Double> ranks = new AppendOnlyMap<>(1024);
        for (int i = 0; i < arrayLinks.length; i++)
            ranks.update(arrayLinks[i]._1(), 1.0);
        for (int i = 0; i < iterations; i++) {
            AppendOnlyMap<Integer, Pair> joinHashMap = new AppendOnlyMap<>(1024);
            for (int j = 0; j < arrayLinks.length; j++) {
                int key = arrayLinks[j]._1();
                ArrayList<Integer> urls = arrayLinks[j]._2();
                Pair tmpJoin = new Pair(null, urls);
                joinHashMap.update(key, tmpJoin);
            }
            scala.collection.Iterator<Tuple2<Integer, Double>> iter = ranks.iterator();
            while (iter.hasNext()) {
                Tuple2<Integer, Double> keyValue = iter.next();
//                Pair pair = joinHashMap.apply(keyValue._1);
//                if(pair != null){
//                    pair.set1(keyValue._2 * 0.85 + 0.15);
//                }else{
//                    joinHashMap.update(keyValue._1, new Pair(keyValue._2 * 0.85 + 0.15, null));
//                }
                updateFunc3.rank = keyValue._2() * 0.85 + 0.15;
                joinHashMap.changeValue(keyValue._1(), updateFunc3);
            }
            AppendOnlyMap<Integer, Double> reduceHashMap = new AppendOnlyMap<>(1024);
            scala.collection.Iterator<Tuple2<Integer, Pair>> iter2 = joinHashMap.iterator();
            while (iter2.hasNext()) {
                Pair rankValue = iter2.next()._2();
                if (rankValue._1 != null && rankValue._2 != null) {
                    ArrayList<Integer> urls = rankValue._2;
                    double newValue = rankValue._1 / urls.size();
                    for (Integer url : urls) {
//                        Double value = reduceHashMap.apply(url);
//                        if(value != null){
//                            reduceHashMap.update(url, value + newValue);
//                        }else{
//                            reduceHashMap.update(url, newValue);
//                        }
                        updateFunc2.adder = newValue;
                        reduceHashMap.changeValue(url, updateFunc2);
                    }
                }
            }
            ranks = reduceHashMap;
        }
        //System.out.println(ranks);
    }

}
