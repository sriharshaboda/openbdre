package com.wipro.ats.bdre.md.rest.util;

import com.wipro.ats.bdre.md.api.base.MetadataAPIBase;
import com.wipro.ats.bdre.md.dao.MessagesDAO;
import com.wipro.ats.bdre.md.dao.jpa.Messages;
import com.wipro.ats.bdre.md.rest.RestWrapper;
import com.wipro.ats.bdre.md.rest.ext.DataLoadAPI;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.LinkedHashMap;

/**
 * Created by cloudera on 5/21/17.
 */
@Controller
@RequestMapping("/message")
public class MessageSchemaAPI extends MetadataAPIBase {

    private static final Logger LOGGER = Logger.getLogger(DataLoadAPI.class);

    @Autowired
    MessagesDAO messagesDAO;

    @RequestMapping(value = {"/createjobs"}, method = RequestMethod.POST)
    @ResponseBody
    public RestWrapper createJob(HttpServletRequest request, Principal principal) {
        String query="";
        String tmp1="";
        StringBuilder buffer = null;
        try {
            buffer = new StringBuilder();
            BufferedReader reader = request.getReader();
            while ((tmp1 = reader.readLine()) != null) {
                buffer.append(tmp1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            query = java.net.URLDecoder.decode(new String(buffer), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String[] linkedList=query.split("&");
        LinkedHashMap<String, String> map=new LinkedHashMap<>();
        for (int i=0;i<linkedList.length;i++)
        {
            String[] tmp=linkedList[i].split("=");
            if (tmp.length==2)
                map.put(tmp[0],tmp[1]);
            else
                map.put(tmp[0],"");
        }
        LOGGER.info(" value of map is " + map.toString());
        RestWrapper restWrapper = null;
        String messageName="";
        String messageFormat="";
        String schema="";
        for (String string : map.keySet()) {
            if (string.startsWith("rawtablecolumn_")) {
                String columnName = string.replaceAll("rawtablecolumn_", "");
                String dataType = map.get(string);
                LOGGER.info("column name is " + columnName + " datatype is " + dataType);
                    schema = schema + "," + columnName + ":" + dataType;
                LOGGER.info("schema is " + schema);
            }
            if (string.startsWith("fileformat_")) {
                if (string.endsWith("messageName")) {
                    messageName = map.get(string);
                    LOGGER.info("messageName is " + messageName);
                } else {
                    messageFormat = map.get(string);
                    LOGGER.info("messageFormat is " + messageFormat);
                }
            }
        }
            Messages messages=new Messages();
            messages.setMessagename(messageName);
            messages.setFormat(messageFormat);
            messages.setMessageSchema(schema.substring(1,schema.length()));
            LOGGER.info(messageName+" "+messageFormat+" "+schema);
            messagesDAO.insert(messages);
        restWrapper = new RestWrapper(null, RestWrapper.OK);
        LOGGER.info("Process and Properties for data load process inserted by" + principal.getName());


        return restWrapper;
    }

    @Override
    public Object execute(String[] params) {
        return null;
    }
    // Read from request
}
