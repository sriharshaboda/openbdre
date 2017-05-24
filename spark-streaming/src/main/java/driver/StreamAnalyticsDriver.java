package driver;

import com.wipro.ats.bdre.GetParentProcessType;
import com.wipro.ats.bdre.md.api.GetProcess;
import com.wipro.ats.bdre.md.api.GetProperties;
import com.wipro.ats.bdre.md.beans.GetPropertiesInfo;
import com.wipro.ats.bdre.md.beans.ProcessInfo;
import com.wipro.ats.bdre.md.dao.MessagesDAO;
import com.wipro.ats.bdre.md.dao.jpa.Messages;
import datasources.KafkaSource;
import emitters.HDFSEmitter;
import kafka.serializer.StringDecoder;
import messageformat.RegexParser;
import messageformat.DelimitedTextParser;
import messageschema.SchemaReader;
import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;
import transformations.Filter;
import transformations.Union;

import java.io.Serializable;
import java.util.*;


/**
 * Created by cloudera on 5/18/17.
 */
public class StreamAnalyticsDriver implements Serializable {
    @Autowired
   static MessagesDAO messagesDAO;

    public static Map<Integer, Set<Integer>> prevMap = new HashMap<>();
    public Map<Integer, DataFrame> pidDataFrameMap = new HashedMap();
    public static List<Integer> listOfSourcePids = new ArrayList<>();
    public static List<Integer> listOfTransformations = new ArrayList<>();
    public static List<Integer> listOfEmitters = new ArrayList<>();
    public static Map<Integer, String> nextPidMap = new HashMap<Integer, String>();
    public static Map<Integer,String> pidMessageTypeMap = new HashedMap();

    public static Integer parentProcessId = new Integer(151);
    static int countEmitterCovered = 0;
    Integer sourcePid = 0;

    public static void main(String[] args) {
        Integer parentProcessId = Integer.parseInt(args[0]);
        GetProcess getProcess = new GetProcess();
        List<ProcessInfo> subProcessList = getProcess.execute(args);
        for(ProcessInfo processInfo:subProcessList)
        {
            nextPidMap.put(processInfo.getProcessId(),processInfo.getNextProcessIds());
            GetParentProcessType getParentProcessType=new GetParentProcessType();
            String processTypeName=getParentProcessType.processTypeName(processInfo.getProcessId());
            if(processTypeName.contains("source"))
            {
                listOfSourcePids.add(processInfo.getProcessId());


                GetProperties getProperties=new GetProperties();
                List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(parentProcessId.toString(),"message");
                if (propertiesInfoList!=null && propertiesInfoList.get(0)!=null)
                {
                    String messageName=propertiesInfoList.get(0).getValue();
                    Messages messages=messagesDAO.get(messageName);
                    pidMessageTypeMap.put(processInfo.getProcessId(),messages.getFormat());
                }

            }
             if(processTypeName.contains("operator"))
            {
                listOfTransformations.add(processInfo.getProcessId());
            }
            if(processTypeName.contains("destination"))
            {
                listOfEmitters.add(processInfo.getProcessId());
            }

        }

        //pidMessageTypeMap.put(152,"Regex");
        //pidMessageTypeMap.put(153,"Delimited");

        List<Integer> currentUpstreamList = new ArrayList<>();
        currentUpstreamList.addAll(listOfSourcePids);
        //populate prevMap with source pids,
        for (Integer sourcePid : listOfSourcePids) {
            prevMap.put(sourcePid, null);
        }
        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Broadcast<Map<Integer,String>> broadcastVar = sc.broadcast(pidMessageTypeMap);

        //TODO: Fetch batchDuration property from database
        GetProperties getProperties=new GetProperties();
        List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(parentProcessId.toString(),"batchDuration");
        long batchDuration = 10000;
          if (propertiesInfoList!=null && propertiesInfoList.get(0)!=null)
             batchDuration = Long.parseLong(propertiesInfoList.get(0).getValue());


        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(batchDuration));
        StreamAnalyticsDriver streamAnalyticsDriver = new StreamAnalyticsDriver();
        //iterate till the list contains only one element and the element must be the parent pid indicating we have reached the end of pipeline
        while (!currentUpstreamList.isEmpty()) {
            System.out.println("currentUpstreamList = " + currentUpstreamList);

            System.out.println("prevMap = " + prevMap);
            streamAnalyticsDriver.identifyFlows(currentUpstreamList, nextPidMap);
        }
        streamAnalyticsDriver.createDataFrames(ssc, listOfSourcePids, prevMap, nextPidMap,broadcastVar);
        ssc.addStreamingListener(new JobListener());
        ssc.start();
        ssc.awaitTermination();
    }

    public void identifyFlows(List<Integer> currentUpstreamList, Map<Integer, String> nextPidMap) {
        //prevMapTemp holds the prev ids only for pids involved in current iteration
        Map<Integer, Set<Integer>> prevMapTemp = new HashMap<>();
        for (Integer currentPid : currentUpstreamList) {

            String nextPidString = nextPidMap.get(currentPid);
            //splitting next process id with comma into string array
            String[] nextPidArray = nextPidString.split(",");
            //new array of integers to hold next pids
            int[] nextPids = new int[nextPidArray.length];
            //iterate through the string array to construct the prev map
            for (int i = 0; i < nextPidArray.length; i++) {
                //cast String to Integer
                nextPids[i] = Integer.parseInt(nextPidArray[i]);
                //populate prevMap with nextPid of currentPid
                new StreamAnalyticsDriver().add(nextPids[i], currentPid, prevMapTemp);
                new StreamAnalyticsDriver().add(nextPids[i], currentPid, prevMap);
            }
        }

        //update the currentUpstreamList with the keys of the prevMap i.e all unique next ids of current step will be upstreams of following iteration
        currentUpstreamList.clear();
        //if the set contains parentProcessId, remove it
        if (prevMapTemp.containsKey(parentProcessId))
            prevMapTemp.remove(parentProcessId);
        currentUpstreamList.addAll(prevMapTemp.keySet());
    }

    //method to add previous pids as values to list against given process-id as key
    public void add(Integer key, Integer newValue, Map<Integer, Set<Integer>> prevMap) {
        Set<Integer> currentValue = prevMap.get(key);
        if (currentValue == null) {
            currentValue = new HashSet<>();
            prevMap.put(key, currentValue);
        }
        currentValue.add(newValue);
    }

    //this method creates dataframes based on the prev map & handles logic accordingly for source/transformation/emitter
    public void createDataFrames(JavaStreamingContext ssc, List<Integer> listOfSourcePids, Map<Integer, Set<Integer>> prevMap, Map<Integer, String> nextPidMap, Broadcast<Map<Integer,String>> broadcastVar) {
        System.out.println("prevMap = " + prevMap);
        //iterate through each source and create respective dataFrames for sources
        for (Integer pid : listOfSourcePids) {
            System.out.println("pid = " + pid);
            System.out.println("Creating DStream for source pid= " + pid);
            //Found Source node, need to create DStream and cast to Dataframe
            //Fetch Source Stream Type & Source Message Type from DB
            String sourceType = "Kafka";

            if (sourceType.equals("Kafka")) {
                System.out.println("inside source  = " + sourceType);
                Map<String, String> kafkaParams = KafkaSource.getKafkaParams(pid);
                Set<String> topics = KafkaSource.getTopics(pid);
                JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
                JavaDStream<String> msgDataStream = directKafkaStream.map(new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple2) {
                        return tuple2._2();
                    }
                });
                msgDataStream.foreachRDD(
                        new Function2<JavaRDD<String>, Time, Void>() {
                            @Override
                            public Void call(JavaRDD<String> rdd, Time time) {
                                sourcePid = pid;
                                System.out.println("sourcePid at beginning of source= " + sourcePid);
                                // Get the singleton instance of SparkSession
                                SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
                                // Convert RDD[String] to RDD[Row] to DataFrame
                                JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
                              @Override
                              public Row call(String record) {
                                  System.out.println("Inside message handler,sourcePid = " + pid);
                                  Object[] attributes = new Object[]{};
                                  //TODO: fetch messageType from sourcePid variable
                                  String messageType="";
                                  GetProperties getProperties=new GetProperties();
                                  List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(parentProcessId.toString(),"message");
                                  if (propertiesInfoList!=null && propertiesInfoList.get(0)!=null)
                                  {
                                      String messageName=propertiesInfoList.get(0).getValue();
                                      Messages messages=messagesDAO.get(messageName);
                                      messageType=messages.getFormat();
                                  }

                                  //String messageType = broadcastVar.value().get(pid);
                                  //String messageType = pidMessageTypeMap.get(pid);
                                  //String messageType = "Regex";
                                  //TODO: Add logic to handle other message types like delimited, etc..
                                  if (messageType.equals("Regex")) {
                                      attributes = new RegexParser().parseRecord(record, pid);
                                  } else if (messageType.equals("Delimited")) {
                                      attributes = new DelimitedTextParser().parseRecord(record, pid);
                                  }
                                  return RowFactory.create(attributes);
                              }
                          }
                                );
                                SchemaReader schemaReader = new SchemaReader();
                                StructType schema = schemaReader.generateSchema(pid);
                                DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schema);
                                //dataFrame.show();
                                pidDataFrameMap.put(pid, dataFrame);
                                System.out.println("pidDataFrameMap = " + pidDataFrameMap);
                                transformAndEmit(nextPidMap.get(pid), pidDataFrameMap);
                                return null;
                            }
                        });

            }
            //TODO: Add logic to handle other Source Types
        }
    }

    public void transformAndEmit(String nextPidString, Map<Integer, DataFrame> pidDataFrameMap) {
        System.out.println("nextPidString = " + nextPidString);
        System.out.println("pidDataFrameMap = " + pidDataFrameMap);
        if (!nextPidString.equals(nextPidMap.get(parentProcessId))) { //condition occurs when all emitters are finished and next is set to parentprocessid
            String[] nextPidStringArray = nextPidString.split(",");
            Integer[] nextPidInts = new Integer[nextPidStringArray.length];
            for (int i = 0; i < nextPidStringArray.length; i++) {
                //cast String to Integer
                nextPidInts[i] = Integer.parseInt(nextPidStringArray[i]);
                System.out.println("nextPidInts[i] = " + nextPidInts[i]);
                if (nextPidInts[i].equals(parentProcessId)) {
                    countEmitterCovered++;
                    System.out.println("No.of Emitters covered =" + countEmitterCovered);
                    if (countEmitterCovered >= listOfEmitters.size()) {
                        System.out.println("clearing contents of pidDataFrameMap before clearing= " + pidDataFrameMap);
                        pidDataFrameMap.clear();
                        System.out.println("clearing contents of pidDataFrameMap before clearing= " + pidDataFrameMap);
                        System.out.println("resetting countEmitterCovered");
                        countEmitterCovered = 0;
                        return;
                    }
                }
            }
            for (int i = 0; i < nextPidInts.length; i++) {
                for (Integer prevPid : prevMap.get(nextPidInts[i])) {
                    if (pidDataFrameMap.get(prevPid) == null) {
                        return;
                    }
                }
            }
            for (Integer pid : nextPidInts) {
                System.out.println("pid for transformation or emitter= " + pid);
                if (listOfTransformations.contains(pid)) {
                    //this pid is of type transformation, find prev pids to output the appropriate dataframe
                    Set<Integer> prevPids = prevMap.get(pid);
                    if (prevPids.size() > 1) {
                        //obtain list of corresponding prevDataFrames for all prevPids
                        DataFrame[] prevDataFrames = new DataFrame[prevPids.size()];
                        GetParentProcessType getParentProcessType=new GetParentProcessType();
                        String processTypeName=getParentProcessType.processTypeName(pid);
                        String transformationType=processTypeName.replace("operator_","");
                        //String transformationType = "union";
                        if (transformationType.equals("union")) {
                            Union union = new Union();
                            DataFrame dataFramePostTransformation = union.transform(pidDataFrameMap, prevMap, pid);
                            pidDataFrameMap.put(pid, dataFramePostTransformation);
                        }
                        //this transformation involves multiple upstream dataframes, e.g: join or union etc.
                        //find the transformation type and create dataframe accordingly
                    } else {
                        //this transformation contains only one upstream pid
                        List<Integer> prevPidList = new ArrayList<Integer>();
                        prevPidList.addAll(prevMap.get(pid));
                        Integer prevPid = prevPidList.get(0);
                        //TODO: Fetch the transformation type from DB
                        String transformationType = "filter";
                        GetParentProcessType getParentProcessType=new GetParentProcessType();
                        transformationType=getParentProcessType.processTypeName(pid).replace("operator_","");
                        if (transformationType.equals("filter")) {
                            Filter filter = new Filter();
                            DataFrame dataFramePostTransformation = filter.transform(pidDataFrameMap, prevMap, pid);
                            pidDataFrameMap.put(pid, dataFramePostTransformation);
                        }
                    }
                }
                if (listOfEmitters.contains(pid)) {
                    //found emitter node, so get upstream pid and persist based on emitter
                    Set<Integer> prevPids = prevMap.get(pid);
                    int count = 0;
                    for (Integer prevPid : prevPids) {
                        count++;
                        System.out.println("count = " + count);
                        System.out.println("currently trying to emit dataframe of prevPid = " + prevPid);
                        DataFrame prevDataFrame = pidDataFrameMap.get(prevPid);
                        String emitterType = "HDFSEmitter";
                        if (emitterType.equals("HDFSEmitter")) {
                            HDFSEmitter hdfsEmitter = new HDFSEmitter();
                            hdfsEmitter.persist(prevDataFrame, pid, prevPid);
                        }
                        //TODO: Handle logic for other emitters
                        //pidDataFrameMap.remove(prevPid);
                    }
                    //pidDataFrameMap.clear();
                }

                transformAndEmit(nextPidMap.get(pid), pidDataFrameMap);
            }
        }

    }

}

