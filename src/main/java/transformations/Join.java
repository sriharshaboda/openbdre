package transformations;

import org.apache.spark.sql.DataFrame;

import java.util.List;
import java.util.Map;

/**
 * Created by cloudera on 5/22/17.
 */
public class Join implements Transformation {
    @Override
    public DataFrame transform(Map<Integer,DataFrame> prevDataFrameMap, Map<Integer,List<Integer>> prevMap, Integer pid){
        Integer prevPid1 = prevMap.get(pid).get(0);
        Integer prevPid2 = prevMap.get(pid).get(1);
        System.out.println("Inside join prevPid1 = " + prevPid1);
        System.out.println("Inside join prevPid2 = " + prevPid2);
        DataFrame prevDF1 = prevDataFrameMap.get(prevPid1);
        DataFrame prevDF2 = prevDataFrameMap.get(prevPid2);
        DataFrame joinedDF = null;
        if(prevDF1!=null & prevDF2!=null)
         joinedDF = prevDF1.unionAll(prevDF2);


        return joinedDF;
    }
}
