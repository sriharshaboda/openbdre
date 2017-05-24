package transformations;

import com.wipro.ats.bdre.md.api.GetProperties;
import com.wipro.ats.bdre.md.beans.GetPropertiesInfo;
import org.apache.spark.sql.DataFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by cloudera on 5/21/17.
 */
public class Filter implements Transformation {
    @Override
    public DataFrame transform(Map<Integer,DataFrame> prevDataFrameMap, Map<Integer,Set<Integer>> prevMap, Integer pid){
        //TODO: fetch the filter logic from DB
        List<Integer> prevPidList = new ArrayList<>();
        prevPidList.addAll(prevMap.get(pid));
        Integer prevPid = prevPidList.get(0);
        System.out.println("Inside filter prevPid = " + prevPid);
        DataFrame prevDataFrame = prevDataFrameMap.get(prevPid);
        DataFrame filteredDF =null;
        GetProperties getProperties=new GetProperties();
        List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(pid.toString(),"kafka");
        String check="";
        String filterValue=new String();
        String colName=new String();
        for(GetPropertiesInfo getPropertiesInfo:propertiesInfoList)
        {
            if(getPropertiesInfo.getKey().equals("operator"))
            {
                check=getPropertiesInfo.getValue();
            }
            if(getPropertiesInfo.getKey().equals("filtervalue"))
            {
                filterValue= getPropertiesInfo.getValue();
            }
            if(getPropertiesInfo.getKey().equals("column"))
            {
                colName= getPropertiesInfo.getValue();
            }
        }

        if(prevDataFrame!=null){
            if (check.equals("equals"))
                filteredDF = prevDataFrame.filter(prevDataFrame.col(colName).equalTo(filterValue));
            else
            filteredDF = prevDataFrame.filter(prevDataFrame.col(colName).gt(filterValue));
        }
        filteredDF.show();
        return filteredDF;
    }
}