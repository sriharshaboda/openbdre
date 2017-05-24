package transformations;

import org.apache.spark.sql.DataFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by cloudera on 5/22/17.
 */
public class Union implements Transformation {
    @Override
    public DataFrame transform(Map<Integer,DataFrame> prevDataFrameMap, Map<Integer,Set<Integer>> prevMap, Integer pid){
        List<Integer> prevPidList = new ArrayList<>();
        prevPidList.addAll(prevMap.get(pid));
        Integer prevPid1 = prevPidList.get(0);
        Integer prevPid2 = prevPidList.get(1);
        System.out.println("Inside join prevPid1 = " + prevPid1);
        System.out.println("Inside join prevPid2 = " + prevPid2);
        DataFrame prevDF1 = prevDataFrameMap.get(prevPid1);
        DataFrame prevDF2 = prevDataFrameMap.get(prevPid2);
        DataFrame unionedDF = null;
        if(prevDF1!=null & prevDF2!=null)
         unionedDF = prevDF1.unionAll(prevDF2);
         unionedDF.show();

        return unionedDF;
    }
}
