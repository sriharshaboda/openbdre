package emitters;

import com.wipro.ats.bdre.md.api.GetProperties;
import com.wipro.ats.bdre.md.beans.GetPropertiesInfo;
import org.apache.spark.sql.DataFrame;

import java.util.Date;
import java.util.List;

/**
 * Created by cloudera on 5/21/17.
 */
public class HDFSEmitter {

    public void persist(DataFrame df, Integer pid, Integer prevPid){
        String hdfsPath = new String();
        System.out.println("Inside emitter hdfs, persisting pid = " + prevPid);
        GetProperties getProperties=new GetProperties();
        List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(pid.toString(),"kafka");
        for(GetPropertiesInfo getPropertiesInfo:propertiesInfoList) {
            if (getPropertiesInfo.getKey().equals("hdfs_path")) {
                hdfsPath = getPropertiesInfo.getValue();
            }
        }
        if(hdfsPath==null || hdfsPath.isEmpty()){
            hdfsPath="/user/cloudera/spark-streaming-data/";
        }
        long date = new Date().getTime();
        if(df.rdd().isEmpty())
            System.out.println("dataframe is empty");
        else{
            System.out.println("Not empty - dataframe is non empty");
            df.show();
        }

        if(df!=null && !df.rdd().isEmpty())
            df.rdd().saveAsTextFile(hdfsPath+ date+"_"+pid+"/");
    }
}