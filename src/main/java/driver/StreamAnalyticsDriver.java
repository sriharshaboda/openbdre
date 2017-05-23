package driver;

import datasources.KafkaSource;
import emitters.HDFSEmitter;
import kafka.serializer.StringDecoder;
import messageformat.ApacheLogRegexParser;
import messageschema.SchemaReader;
import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
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

import java.io.Serializable;
import java.util.*;


/**
 * Created by cloudera on 5/18/17.
 */
public class StreamAnalyticsDriver implements Serializable{

    public static Map<Integer, List<Integer>> prevMap = new HashMap<>();
    public static Map<Integer, DataFrame> pidDataFrameMap = new HashedMap();
    public static List<Integer> listOfSourcePids = new ArrayList<>();
    public static List<Integer> listOfTransformations = new ArrayList<>();
    public static List<Integer> listOfEmitters = new ArrayList<>();
    public static Map<Integer, String> nextPidMap = new HashMap<Integer, String>();
    public static Integer parentProcessId = new Integer(151);
    public static void main(String[] args) {
        //Integer parentProcessId = Integer.parseInt(args[0]);
        parentProcessId = 151;
        listOfSourcePids.add(152);
        listOfSourcePids.add(153);


        listOfTransformations.add(154);
        listOfTransformations.add(155);
        listOfTransformations.add(156);
        listOfTransformations.add(157);


        listOfEmitters.add(158);
        listOfEmitters.add(159);


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
        for (Integer sourcePid : listOfSourcePids) {
            prevMap.put(sourcePid, null);
        }
        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //TODO: Fetch batchDuration property from database
        long batchDuration = 10000;
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(batchDuration));

        //iterate till the list contains only one element and the element must be the parent pid indicating we have reached the end of pipeline
        while (currentUpstreamList.size() != 1 || !currentUpstreamList.contains(parentProcessId)) {
            System.out.println("currentUpstreamList = " + currentUpstreamList);
            StreamAnalyticsDriver streamAnalyticsDriver = new StreamAnalyticsDriver();
            System.out.println("prevMap = " + prevMap);
            streamAnalyticsDriver.createDataFrames(ssc, currentUpstreamList, prevMap,nextPidMap);
            streamAnalyticsDriver.identifyFlows(currentUpstreamList, nextPidMap);
        }
        ssc.start();
        ssc.awaitTermination();
    }

    public void identifyFlows(List<Integer> currentUpstreamList, Map<Integer, String> nextPidMap) {
        //prevMapTemp holds the prev ids only for pids involved in current iteration
        Map<Integer, List<Integer>> prevMapTemp = new HashMap<>();
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
        currentUpstreamList.addAll(prevMapTemp.keySet());
    }

    //method to add previous pids as values to list against given process-id as key
    public void add(Integer key, Integer newValue, Map<Integer, List<Integer>> prevMap) {
        List<Integer> currentValue = prevMap.get(key);
        if (currentValue == null) {
            currentValue = new ArrayList<>();
            prevMap.put(key, currentValue);
        }
        currentValue.add(newValue);
    }

    //this method creates dataframes based on the prev map & handles logic accordingly for source/transformation/emitter
    public void createDataFrames(JavaStreamingContext ssc, List<Integer> currentUpstreamList, Map<Integer, List<Integer>> prevMap,Map<Integer, String> nextPidMap) {
        System.out.println("prevMap = " + prevMap);
        //iterate through each upstream and create respective dataframes based on prev value if prevMap
        for (Integer pid : currentUpstreamList) {
            final Integer iteratorPid = pid;
            System.out.println("pid = " + pid);
            if (listOfSourcePids.contains(pid)) {
                System.out.println("after printing pid inside loop= " + pid);
                //Found Source node, need to create DStream and cast to Dataframe
                //Fetch Source Stream Type & Source Message Type from DB
                String sourceType = "Kafka";

                if (sourceType.equals("Kafka")) {
                    System.out.println("inside source  = " + sourceType);
                    Map<String, String> kafkaParams = KafkaSource.getKafkaParams(pid);
                    Set<String> topics = KafkaSource.getTopics(pid);
                    JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
                    JavaDStream<String> msgDataStream = directKafkaStream.map(new FlattenKafkaMessage());
                    msgDataStream.foreachRDD(
                            new Function2<JavaRDD<String>, Time, Void>() {
                                @Override
                                public Void call(JavaRDD<String> rdd, Time time) {

                                    // Get the singleton instance of SparkSession
                                    SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());

                                    // Convert RDD[String] to RDD[Row] to DataFrame
                                    JavaRDD<Row> rowRDD = rdd.map(new MessageTypeHandler());
                                    SchemaReader schemaReader = new SchemaReader();
                                    StructType schema = schemaReader.generateSchema(pid);
                                    System.out.println("schema = " + schema);
                                    DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schema);
                                    dataFrame.show();
                                    pidDataFrameMap.put(pid, dataFrame);
                                    System.out.println("pidDataFrameMap = " + pidDataFrameMap);
                                    transformAndEmit(nextPidMap.get(pid),pidDataFrameMap);
                                    return null;
                                }
                            });

                }
                //TODO: Add logic to handle other Source Types
            } /* if (listOfTransformations.contains(pid)) {
                //this pid is of type transformation, find prev pids to output the appropriate dataframe
                List<Integer> prevPids = prevMap.get(pid);
                if (prevPids.size() > 1) {
                    //obtain list of corresponding prevDataFrames for all prevPids
                    DataFrame[] prevDataFrames = new DataFrame[prevPids.size()];

                    String transformationType = "join";
                    if (transformationType.equals("join")) {
                        Join join = new Join();
                        for (Integer prevPid : prevPids) {
                            DataFrame prevDataFrame = pidDataFrameMap.get(prevPid);

                        }
                        DataFrame dataFramePostTransformation = join.transform(pidDataFrameMap, prevMap, pid);
                        pidDataFrameMap.put(pid, dataFramePostTransformation);
                    }
                    //this transformation involves multiple upstream dataframes, e.g: join or union etc.
                    //find the transformation type and create dataframe accordingly
                } else {
                    //this transformation contains only one upstream pid
                    Integer prevPid = prevMap.get(pid).get(0);
                    //TODO: Fetch the transformation type from DB
                    String transformationType = "filter";
                    if (transformationType.equals("filter")) {
                        Filter filter = new Filter();
                        DataFrame dataFramePostTransformation = filter.transform(pidDataFrameMap, prevMap, pid);
                        pidDataFrameMap.put(pid, dataFramePostTransformation);
                    }
                }
            }
            if (listOfEmitters.contains(pid)) {
                //found emitter node, so get upstream pid and persist based on emitter
                List<Integer> prevPids = prevMap.get(pid);
                for (Integer prevPid : prevPids) {
                    DataFrame prevDataFrame = pidDataFrameMap.get(prevPid);
                    String emitterType = "HDFSEmitter";
                    if (emitterType.equals("HDFSEmitter")) {
                        HDFSEmitter hdfsEmitter = new HDFSEmitter();
                        hdfsEmitter.persist(prevDataFrame, pid);
                    }
                    //TODO: Handle logic for other emitters
                }
            }*/
        }

    }
    public void transformAndEmit(String nextPidString,Map<Integer,DataFrame> pidDataFrameMap) {
        if (!nextPidString.equals(nextPidMap.get(parentProcessId))){
            String[] nextPidStringArray = nextPidString.split(",");
        Integer[] nextPidInts = new Integer[nextPidStringArray.length];
        for (int i = 0; i < nextPidStringArray.length; i++) {
            //cast String to Integer
            nextPidInts[i] = Integer.parseInt(nextPidStringArray[i]);
        }
        for (int i = 0; i < nextPidInts.length; i++) {
            for (Integer prevPid : prevMap.get(nextPidInts[i])) {
                if (pidDataFrameMap.get(prevPid) == null)
                    return;
            }
        }
        // while()
        for (Integer pid : nextPidInts) {
            if (listOfTransformations.contains(pid)) {
                //this pid is of type transformation, find prev pids to output the appropriate dataframe
                List<Integer> prevPids = prevMap.get(pid);
                if (prevPids.size() > 1) {
                    //obtain list of corresponding prevDataFrames for all prevPids
                    DataFrame[] prevDataFrames = new DataFrame[prevPids.size()];

                    String transformationType = "join";
                    if (transformationType.equals("join")) {
                        Join join = new Join();
                        DataFrame dataFramePostTransformation = join.transform(pidDataFrameMap, prevMap, pid);
                        pidDataFrameMap.put(pid, dataFramePostTransformation);
                    }
                    //this transformation involves multiple upstream dataframes, e.g: join or union etc.
                    //find the transformation type and create dataframe accordingly
                } else {
                    //this transformation contains only one upstream pid
                    Integer prevPid = prevMap.get(pid).get(0);
                    //TODO: Fetch the transformation type from DB
                    String transformationType = "filter";
                    if (transformationType.equals("filter")) {
                        Filter filter = new Filter();
                        DataFrame dataFramePostTransformation = filter.transform(pidDataFrameMap, prevMap, pid);
                        pidDataFrameMap.put(pid, dataFramePostTransformation);
                    }
                }
            }
            if (listOfEmitters.contains(pid)) {
                //found emitter node, so get upstream pid and persist based on emitter
                List<Integer> prevPids = prevMap.get(pid);
                for (Integer prevPid : prevPids) {
                    DataFrame prevDataFrame = pidDataFrameMap.get(prevPid);
                    String emitterType = "HDFSEmitter";
                    if (emitterType.equals("HDFSEmitter")) {
                        HDFSEmitter hdfsEmitter = new HDFSEmitter();
                        hdfsEmitter.persist(prevDataFrame, pid);
                    }
                    //TODO: Handle logic for other emitters
                }
            }
            transformAndEmit(nextPidMap.get(pid), pidDataFrameMap);
        }
    }
    }

    class FlattenKafkaMessage implements Function<Tuple2<String,String>,String>{
        @Override
        public String call(Tuple2<String, String> tuple2) {
            return tuple2._2();
        }
    }

    class MessageTypeHandler implements Function<String, Row> {
        @Override
        public Row call(String record) {
            Object[] attributes = new Object[]{};
            //TODO: Add logic to handle other message types like delimited, etc..
            String messageType = "ApacheLog";
            if (messageType.equals("ApacheLog")) {
                attributes = new ApacheLogRegexParser().parseRecord(record);
                System.out.println("attributes = " + attributes);
            }
            return RowFactory.create(attributes);
        }
    }

}

