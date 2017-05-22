package transformations;

import org.apache.spark.sql.DataFrame;

/**
 * Created by cloudera on 5/21/17.
 */
public interface Transformation {
    public DataFrame transform(DataFrame df);
}
