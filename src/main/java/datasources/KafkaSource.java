package datasources;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by cloudera on 5/19/17.
 */
public class KafkaSource {

    Map<String,String> kafkaParams;
    Set<String> topics;

    public static Map<String,String> getKafkaParams(int pid){
        Map<String,String> kafkaParams = new  HashMap<String,String>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        return kafkaParams;
        }

    public static Set<String> getTopics(int pid){
        Set<String> topics = new HashSet<>();
        topics.add("test");
        return topics;
    }

}
