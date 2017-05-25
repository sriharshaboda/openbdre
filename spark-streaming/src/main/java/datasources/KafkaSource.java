package datasources;

import com.wipro.ats.bdre.md.api.GetProperties;
import com.wipro.ats.bdre.md.beans.GetPropertiesInfo;

import java.util.*;

/**
 * Created by cloudera on 5/19/17.
 */
public class KafkaSource {

    Map<String,String> kafkaParams;
    Set<String> topics = new HashSet<>();
    String topicName=new String();

    //TODO fetch props from DB
    public Map<String,String> getKafkaParams(int pid){
        GetProperties getProperties=new GetProperties();
        Map<String,String> kafkaParams = new  HashMap<String,String>();
       // List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(String.valueOf(pid),"kafka");
        Properties kafkaProperties=  getProperties.getProperties(String.valueOf(pid),"kafka");
        Enumeration e = kafkaProperties.propertyNames();
        if (!kafkaProperties.isEmpty()) {
            while (e.hasMoreElements()) {
                String key = (String) e.nextElement();
                kafkaParams.put(key,kafkaProperties.getProperty(key));
            }
        }

        kafkaParams.put("metadata.broker.list", "localhost:9092");
        return kafkaParams;
    }

    public Set<String> getTopics(int pid){
        GetProperties getProperties=new GetProperties();
        Properties kafkaProperties=  getProperties.getProperties(String.valueOf(pid),"kafka");
        topicName = kafkaProperties.getProperty("Topic Name");
        String[] topicArray = topicName.split(",");
        for(int i=0;i<topicArray.length;i++){
            System.out.println("topic = " + topicArray[i]);
            topics.add(topicArray[i]);
        }
        return topics;
    }

}
