package transformations;

import org.apache.spark.sql.DataFrame;

import java.util.List;
import java.util.Map;

/**
 * Created by cloudera on 5/21/17.
 */
public interface Transformation {
    public DataFrame transform(Map<Integer,DataFrame> prevDataFrameMap, Map<Integer,List<Integer>> prevMap, Integer pid);
}
