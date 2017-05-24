package datasources;

import com.wipro.ats.bdre.md.api.GetProperties;
import com.wipro.ats.bdre.md.beans.GetPropertiesInfo;

import java.util.*;

/**
 * Created by cloudera on 5/19/17.
 */
public class KafkaSource {

    Map<String,String> kafkaParams;
    static Set<String> topics = new HashSet<>();
    static String topicName=new String();

    //TODO fetch props from DB
    public static Map<String,String> getKafkaParams(int pid){
        GetProperties getProperties=new GetProperties();
        Map<String,String> kafkaParams = new  HashMap<String,String>();
        List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(String.valueOf(pid),"kafka");
        for(GetPropertiesInfo getPropertiesInfo:propertiesInfoList)
        {
            if (!getPropertiesInfo.getValue().equals(""))
                kafkaParams.put(getPropertiesInfo.getKey(),getPropertiesInfo.getValue());
        }
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        return kafkaParams;
    }

    public static Set<String> getTopics(int pid){
        GetProperties getProperties=new GetProperties();
        Map<String,String> kafkaParams = new  HashMap<String,String>();
        List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(String.valueOf(pid),"kafka");
        for(GetPropertiesInfo getPropertiesInfo:propertiesInfoList)
        {
            if (getPropertiesInfo.getKey().equals("Topic Name"))
                topicName=getPropertiesInfo.getValue();
        }
        String[] topicArray = topicName.split(",");
        for(int i=0;i<topicArray.length;i++){
            System.out.println("topic = " + topicArray[i]);
            topics.add(topicArray[i]);
        }
        return topics;
    }

}
