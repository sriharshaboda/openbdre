package emitters;

import org.apache.spark.sql.DataFrame;

import java.util.Date;

/**
 * Created by cloudera on 5/21/17.
 */
public class HDFSEmitter {

    public void persist(DataFrame df, Integer pid, Integer prevPid){
        System.out.println("Inside emitter hdfs, persisting pid = " + prevPid);
        long date = new Date().getTime();
        if(df.rdd().isEmpty())
            System.out.println("dataframe is empty");
        else{
            System.out.println("Not empty - dataframe is non empty");
            df.show();
        }

        if(df!=null && !df.rdd().isEmpty())
            df.rdd().saveAsTextFile("/user/cloudera/spark-streaming-data/"+ date+"_"+pid+"/");
    }
}