package datasources;

import com.wipro.ats.bdre.md.api.GetProperties;

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
    //TODO fetch props from DB
    public static Map<String,String> getKafkaParams(int pid){
        GetProperties getProperties=new GetProperties();
        Map<String,String> kafkaParams = new  HashMap<String,String>();
       /* List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(String.valueOf(pid),"kafka");
        for(GetPropertiesInfo getPropertiesInfo:propertiesInfoList)
        {
            if (!getPropertiesInfo.getValue().equals(""))
                kafkaParams.put(getPropertiesInfo.getKey(),getPropertiesInfo.getValue());
        }*/
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        return kafkaParams;
    }

    public static Set<String> getTopics(int pid){
        Set<String> topics = new HashSet<>();
        topics.add("test2");
        return topics;
    }

}
