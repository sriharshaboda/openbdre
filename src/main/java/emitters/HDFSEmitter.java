package emitters;

import org.apache.spark.sql.DataFrame;

import java.util.Date;

/**
 * Created by cloudera on 5/21/17.
 */
public class HDFSEmitter {

    public void persist(DataFrame df, Integer pid){
        System.out.println("Inside emitter hdfs pid = " + pid);
        long date = new Date().getTime();
        if(df!=null)
        df.rdd().saveAsTextFile("/user/cloudera/spark-streaming-data/"+ date+"/");
    }
}
