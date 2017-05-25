package emitters;

import com.wipro.ats.bdre.md.api.GetProperties;
import com.wipro.ats.bdre.md.beans.GetPropertiesInfo;
import org.apache.spark.sql.DataFrame;

import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 * Created by cloudera on 5/21/17.
 */
public class HDFSEmitter {

    public void persist(DataFrame df, Integer pid, Integer prevPid){
        String hdfsPath = new String();
        System.out.println("Inside emitter hdfs, persisting pid = " + prevPid);
        GetProperties getProperties=new GetProperties();


        Properties hdfsProperties=  getProperties.getProperties(String.valueOf(pid),"kafka");
        hdfsPath = hdfsProperties.getProperty("hdfs_path");
        if(hdfsPath==null || hdfsPath.isEmpty()){
            hdfsPath="/user/cloudera/spark-streaming-data/";
        }
        long date = new Date().getTime();
        if(df.rdd().isEmpty())
            System.out.println("dataframe is empty");
        else{
            System.out.println("Not empty - dataframe is non empty");
            df.show(100);
        }

        if(df!=null && !df.rdd().isEmpty()){
            System.out.println("showing dataframe df before writing to hdfs  ");
            df.show(100);
            df.rdd().saveAsTextFile(hdfsPath+ date+"_"+pid+"/");
            System.out.println("showing dataframe df after writing to hdfs  ");
            df.show(100);
        }
    }
}