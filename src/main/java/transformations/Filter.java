package transformations;

import org.apache.spark.sql.DataFrame;

/**
 * Created by cloudera on 5/21/17.
 */
public class Filter implements Transformation {
    @Override
    public DataFrame transform(DataFrame df){
        DataFrame filteredDF = df.filter(df.col("responseCode").gt("200"));
        return filteredDF;
    }
}
