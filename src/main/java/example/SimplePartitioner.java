package example;

import kafka.producer.Partitioner;

/**
 * Created by pchanumolu on 4/28/15.
 */
public class SimplePartitioner implements Partitioner{

    public SimplePartitioner(){}

    @Override
    public int partition(Object key, int numPartitions) {
        int partition =0;
        String stringKey = (String) key;
        int offSet = stringKey.lastIndexOf(".");
        if(offSet>0){
            return Integer.parseInt(stringKey.substring(offSet+1)) % numPartitions;
        }
        return partition;
    }
}
