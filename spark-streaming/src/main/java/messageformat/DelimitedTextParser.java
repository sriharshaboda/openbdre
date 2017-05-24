package messageformat;

/**
 * Created by cloudera on 5/21/17.
 */
public class DelimitedTextParser implements MessageParser{
     public String[] parseRecord(String record, Integer pid) {
         System.out.println("pid inside delimited log parser = " + pid);
         //TODO: fetch delimiter from DB props
         String delimiter = ",";
         String[] parsedRecords = record.split(delimiter);
         return parsedRecords;
     }
}

