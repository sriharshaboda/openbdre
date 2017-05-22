import datasources.KafkaSource;
import kafka.serializer.StringDecoder;
import messageformat.ApacheLogRegexParser;
import messageschema.SchemaReader;
import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
import scala.Tuple2;
import transformations.Filter;
import transformations.Join;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by cloudera on 5/18/17.
 */
public class Test {

    public static Map<Integer,List<Integer>> prevMap = new HashMap<>();
    public static Map<Integer,DataFrame> pidDataFrameMap = new HashedMap();
    public static List<Integer> listOfSourcePids = new ArrayList<>();
    public static List<Integer> listOfTransformations = new ArrayList<>();
    public static List<Integer> listOfEmitters = new ArrayList<>();

    public static void main(String[] args) {
        //Integer parentProcessId = Integer.parseInt(args[0]);
        Integer parentProcessId = 151;
        listOfSourcePids.add(152);
        listOfSourcePids.add(153);


        listOfTransformations.add(154);
        listOfTransformations.add(155);
        listOfTransformations.add(156);
        listOfTransformations.add(157);


        listOfEmitters.add(158);
        listOfEmitters.add(159);

        Map<Integer, String> nextPidMap = new HashMap<Integer, String>();
        nextPidMap.put(151, "152,153");
        nextPidMap.put(152, "154");
        nextPidMap.put(153, "154,155");
        nextPidMap.put(154, "157");
        nextPidMap.put(155, "156");
        nextPidMap.put(156, "158,159");
        nextPidMap.put(157, "158");
        nextPidMap.put(151, "152,153");
        nextPidMap.put(158, "151");
        nextPidMap.put(159, "151");

        List<Integer> currentUpstreamList = new ArrayList<>();
        currentUpstreamList.addAll(listOfSourcePids);
        //populate prevMap with source pids,
        for(Integer sourcePid:listOfSourcePids){
            prevMap.put(sourcePid,null);
        }
        //iterate till the list contains only one element and the element must be the parent pid indicating we have reached the end of pipeline
        while (currentUpstreamList.size()!=1  || !currentUpstreamList.contains(parentProcessId)) {
            System.out.println("currentUpstreamList = " + currentUpstreamList);
            Test test = new Test();
            System.out.println("prevMap = " + prevMap);
            test.createDataFrames(currentUpstreamList,prevMap);
            test.identifyFlows(currentUpstreamList, nextPidMap);
        }
    }

        public void identifyFlows(List<Integer> currentUpstreamList, Map<Integer,String> nextPidMap){
            //prevMapTemp holds the prev ids only for pids involved in current iteration
            Map<Integer,List<Integer>> prevMapTemp = new HashMap<>();
            for(Integer currentPid:currentUpstreamList){

                    String nextPidString = nextPidMap.get(currentPid);
                    //splitting next process id with comma into string array
                    String[] nextPidArray = nextPidString.split(",");
                    //new array of integers to hold next pids
                    int[] nextPids = new int[nextPidArray.length];
                    //iterate through the string array to construct the prev map
                    for(int i=0;i<nextPidArray.length;i++){
                        //cast String to Integer
                        nextPids[i]=Integer.parseInt(nextPidArray[i]);
                        //populate prevMap with nextPid of currentPid
                        new Test().add(nextPids[i],currentPid,prevMapTemp);
                        new Test().add(nextPids[i],currentPid,prevMap);
                    }
            }

            //update the currentUpstreamList with the keys of the prevMap i.e all unique next ids of current step will be upstreams of following iteration
            currentUpstreamList.clear();
            currentUpstreamList.addAll(prevMapTemp.keySet());
        }

            //method to add previous pids as values to list against given process-id as key
        public void add(Integer key, Integer newValue, Map<Integer,List<Integer>> prevMap) {
            List<Integer> currentValue = prevMap.get(key);
            if (currentValue == null) {
                currentValue = new ArrayList<>();
                prevMap.put(key, currentValue);
            }
            currentValue.add(newValue);
        }

            //this method creates dataframes based on the prev map & handles logic accordingly for source/transformation/emitter
        public void createDataFrames(List<Integer> currentUpstreamList,Map<Integer,List<Integer>> prevMap){
            // Create a Spark Context.
            SparkConf conf = new SparkConf().setAppName("Log Analyzer");
            JavaSparkContext sc = new JavaSparkContext(conf);
            //TODO: Fetch batchDuration property from database
            long batchDuration = 10000;
            JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(batchDuration));

            //iterate through each upstream and create respective dataframes based on prev value if prevMap
            for(Integer pid:currentUpstreamList){
                if(listOfSourcePids.contains(pid)){
                    //Found Source node, need to create DStream and cast to Dataframe
                    //Fetch Source Stream Type & Source Message Type from DB
                    String sourceType = "Kafka";
                    String messageType = "ApacheLog";
                    if (sourceType.equals("Kafka")) {
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

                                        // Get the singleton instance of SparkSession
                                        SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());

                                        // Convert RDD[String] to RDD[Row] to DataFrame
                                        JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
                                            public Row call(String record) {
                                                Object[] attributes = new Object[]{};
                                                //TODO: Add logic to handle other message types like delimited, etc..
                                                if (messageType.equals("ApacheLog")) {
                                                    attributes = new ApacheLogRegexParser().parseRecord(record);
                                                }
                                                return RowFactory.create(attributes);
                                            }
                                        });
                                        SchemaReader schemaReader = new SchemaReader();
                                        StructType schema = schemaReader.generateSchema(pid);
                                        DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schema).cache();
                                        dataFrame.show();
                                        pidDataFrameMap.put(pid, dataFrame);
                                        return null;
                                    }
                                });
                    }
                    //TODO: Add logic to handle other Source Types
                }
                else if(listOfTransformations.contains(pid)){
                    //this pid is of type transformation, find prev pids to output the appropriate dataframe
                    List<Integer> prevPids = prevMap.get(pid);
                    if(prevPids.size()>1){
                        //obtain list of corresponding prevDataFrames for all prevPids
                        DataFrame[] prevDataFrames = new DataFrame[prevPids.size()];

                        String transformationType = "join";
                        if(transformationType.equals("join")){
                            Join join = new Join();
                            for(Integer prevPid:prevPids){
                                DataFrame prevDataFrame=pidDataFrameMap.get(prevPid);

                            }
                            DataFrame dataFramePostTransformation = join.transform(pidDataFrameMap,prevMap,pid);
                            pidDataFrameMap.put(pid,dataFramePostTransformation);
                        }
                        //this transformation involves multiple upstream dataframes, e.g: join or union etc.
                        //find the transformation type and create dataframe accordingly
                    }
                    else{
                        //this transformation contains only one upstream pid
                        Integer prevPid = prevMap.get(pid).get(0);
                        //TODO: Fetch the transformation type from DB
                        String transformationType = "filter";
                        if(transformationType.equals("filter")){
                            Filter filter = new Filter();
                            DataFrame dataFramePostTransformation = filter.transform(pidDataFrameMap,prevMap,pid);
                            pidDataFrameMap.put(pid,dataFramePostTransformation);
                        }
                    }
                }
                else if(listOfEmitters.contains(pid)){
                    //found emitter node, so get upstream pid and persist based on emitter

                }
            }
        }

    }
