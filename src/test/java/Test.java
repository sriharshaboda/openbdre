import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by cloudera on 5/18/17.
 */
public class Test {
    private static final Logger logger = Logger.getLogger("Access");

    private static final String LOG_ENTRY_PATTERN =
            // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
            "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static void main(String... args) {
        String message = "64.242.88.10 - - [07/Mar/2004:16:53:46 -0800] \"GET /twiki/bin/rdiff/TWiki/TWikiRegistration HTTP/1.1\" 200 34395";
        Matcher m = PATTERN.matcher(message);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logline" + message);
            throw new RuntimeException("Error parsing logline");
        }
    }

}
