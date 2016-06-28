package com.wipro.ats.bdre.md.api;

import com.wipro.ats.bdre.md.beans.ProcessInfo;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;


/**
 * Created by cloudera on 6/27/16.
 */
public class GetSubProcessTypeList {
    private static final Logger LOGGER = Logger.getLogger(GetSubProcessTypeList.class);
    public static void main(String args[]){
        GetSubProcessTypeList getSubProcessTypeList = new GetSubProcessTypeList();
        String subProcessList = getSubProcessTypeList.getUniqueListOfSubProcessTypes(args);
        LOGGER.debug("subProcessTypeList = " + subProcessList);
    }
    public String getUniqueListOfSubProcessTypes(String[] params){
        GetProcess getProcess = new GetProcess();
        List<ProcessInfo> processInfoList = getProcess.getSubProcesses(params);
        StringBuilder uniqueSubProcessTypeList = new StringBuilder();
        Set<Integer> subProcessTypeList = new LinkedHashSet<Integer>();
        for(ProcessInfo subProcessInfo: processInfoList){
            subProcessTypeList.add(subProcessInfo.getProcessTypeId());
            //subProcessList.append(subProcessInfo.getProcessTypeId()+",");
        }
        for(Integer subProcessType:subProcessTypeList){
            uniqueSubProcessTypeList.append(subProcessType+",");
        }
        return uniqueSubProcessTypeList.substring(0,uniqueSubProcessTypeList.lastIndexOf(",")); //removing trailing comma
    }

}
