package microBenchmark;

import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by luluorta on 16-4-11.
 */
public abstract class PR {

    public String name;

    static class Pair {
        Double _1;
        ArrayList<Integer> _2;

        Pair(Double _1, ArrayList<Integer> _2) {
            this._1 = _1;
            this._2 = _2;
        }

        public void set1(Double _1) {
            this._1 = _1;
        }

        public void set2(ArrayList<Integer> _2) {
            this._2 = _2;
        }
    }

    static class Chunk extends ByteArrayOutputStream {

        public int capacity() {
            return buf.length;
        }

        public Chunk(int size) {
            super(size);
        }

        public ByteArrayInputStream toInputStream() {
            return new ByteArrayInputStream(buf);
        }
    }

    protected int count = 0;
    protected int keyCount = 0;
    protected int idCount = 0;

    protected abstract void cache(Map<Integer, ArrayList<Integer>> links);

    public void textFile(File file) {
        try {
            TreeMap<Integer, ArrayList<Integer>> links = new TreeMap<>();
            BitSet ids = new BitSet();
            ArrayList<Integer> tmpValueList;
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                count++;
                String[] parts = line.split("\\s+");
                int key = Integer.parseInt(parts[0]);
                int value = Integer.parseInt(parts[1]);
                ids.set(key);
                ids.set(value);
                if (links.containsKey(key)) {
                    tmpValueList = links.get(key);
                } else {
                    tmpValueList = new ArrayList<>();
                    links.put(key, tmpValueList);
                }
                tmpValueList.add(value);
            }
            keyCount = links.size();
            idCount = ids.cardinality();
            count += keyCount;
//            System.out.print("key count: " + keyCount +
//                    ", id count: " + idCount +
//                    ", count " + count + "\n");

            cache(links);
        } catch(IOException e){
            System.out.println(e);
        }
    }

    public abstract void compute(int iterations);

    public void close() {
    }

}
