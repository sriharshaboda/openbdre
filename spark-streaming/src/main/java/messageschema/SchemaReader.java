package messageschema;

import com.wipro.ats.bdre.md.api.GetProcess;
import com.wipro.ats.bdre.md.api.GetProperties;
import com.wipro.ats.bdre.md.beans.GetPropertiesInfo;
import com.wipro.ats.bdre.md.beans.ProcessInfo;
import com.wipro.ats.bdre.md.dao.MessagesDAO;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by cloudera on 5/20/17.
 */
public class SchemaReader {


    @Autowired
    MessagesDAO messagesDAO;
    //TODO: update the schema automatically
    String schemaString = "ipAddress clientIdentd userID dateTimeString method endpoint protocol responseCode contentSize";

    public StructType generateSchema(int pid){
        GetProcess getProcess=new GetProcess();
        ProcessInfo processInfo=getProcess.getProcess(pid);
        GetProperties getProperties=new GetProperties();
        List<GetPropertiesInfo> propertiesInfoList= (List<GetPropertiesInfo>) getProperties.getProperties(String.valueOf(pid),"batchDuration");
        String messageName="";
        if (propertiesInfoList!=null && propertiesInfoList.get(0)!=null)
            messageName=propertiesInfoList.get(0).getValue();
            schemaString=messagesDAO.get(messageName).getMessageSchema();
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