package messageschema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cloudera on 5/20/17.
 */
public class SchemaReader {

    String schemaString = "ipAddress clientIdentd userID dateTimeString method endpoint protocol responseCode contentSize";

    public StructType generateSchema(int pid){
    // Generate the schema based on the string of schema
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : schemaString.split(" ")) {
        StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
        fields.add(field);
    }
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }
}