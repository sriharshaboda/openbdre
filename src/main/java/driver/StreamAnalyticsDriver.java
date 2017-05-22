package driver;

import datasources.KafkaSource;
import kafka.serializer.StringDecoder;
import messageformat.ApacheLogRegexParser;
import messageschema.SchemaReader;
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
import transformations.Transformation;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The driver.StreamAnalyticsDriver takes in an apache access log file and
 * computes some statistics on them.
 * <p>
 * Example command to run:
 * %  ${YOUR_SPARK_HOME}/bin/spark-submit
 * --class "com.databricks.apps.logs.chapter1.LogsAnalyzer"
 * --master local[4]
 * target/log-analyzer-1.0.jar
 * ../../data/apache.accesslog
 */
public class StreamAnalyticsDriver {
    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;

    public static void main(String[] args) {
        //parent-process-id
        String pid = "151";
        int[] listOfSourcePids = {152,153};

        int[] listOfTransformations = {5, 6};
        int[] listOfEmitters = {7};

        // Create a Spark Context.
        SparkConf conf = new SparkConf().setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(10000));
        Map<Integer, DataFrame> sourceDFrameMap = new HashMap<Integer, DataFrame>();
        for (int i = 0; i < listOfSourcePids.length; i++) {
            int sourcePid = listOfSourcePids[i];
            //TODO: get from DB
            String sourceType = "Kafka";
            String messageType = "ApacheLog";
            if (sourceType.equals("Kafka")) {
                Map<String, String> kafkaParams = KafkaSource.getKafkaParams(sourcePid);
                Set<String> topics = KafkaSource.getTopics(sourcePid);
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
                                        if (messageType.equals("ApacheLog")) {
                                            attributes = new ApacheLogRegexParser().parseRecord(record);
                                        }
                                        return RowFactory.create(attributes);
                                    }
                                });
                                SchemaReader schemaReader = new SchemaReader();
                                StructType schema = schemaReader.generateSchema(sourcePid);
                                DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, schema).cache();
                                //dataFrame.show();
                                Filter filter = new Filter();
                                filter.transform(dataFrame).show();
                                sourceDFrameMap.put(sourcePid, dataFrame);
                                return null;
                            }
                        });
            }
        }
        ssc.start();
        ssc.awaitTermination();
    }
}