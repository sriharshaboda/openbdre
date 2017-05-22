package transformations;

import org.apache.spark.sql.DataFrame;

import java.util.List;
import java.util.Map;

/**
 * Created by cloudera on 5/21/17.
 */
public class Filter implements Transformation {
    @Override
    public DataFrame transform(Map<Integer,DataFrame> prevDataFrameMap, Map<Integer,List<Integer>> prevMap, Integer pid){
        //TODO: fetch the filter logic from DB
        Integer prevPid = prevMap.get(pid).get(0);
        DataFrame prevDataFrame = prevDataFrameMap.get(prevPid);
        DataFrame filteredDF =null;
        if(prevDataFrame!=null)
                 filteredDF = prevDataFrame.filter(prevDataFrame.col("responseCode").gt("200"));
        return filteredDF;
    }
}
