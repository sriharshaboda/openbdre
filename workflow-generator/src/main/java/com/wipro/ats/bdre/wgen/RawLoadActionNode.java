/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wipro.ats.bdre.wgen;

import com.sun.org.apache.xalan.internal.xsltc.util.IntegerArray;
import com.wipro.ats.bdre.md.api.GetProperties;
import com.wipro.ats.bdre.md.api.InitJob;
import com.wipro.ats.bdre.md.beans.ProcessInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Created by arijit on 12/21/14.s
 */


/*
Action nodes are the mechanism by which a workflow triggers the execution of a task
Here, we set the id and return name of the action node.
The method getXML() returns a string which contains name, Id, next success node(ToNode) and next failure node(FailNode)
for the current action node, appropriately formatted as XML. 
*/

public class RawLoadActionNode extends GenericActionNode {


    private ProcessInfo processInfo = new ProcessInfo();
    private ActionNode actionNode = null;

    /**
     * This constructor is used to set node id and process information.
     *
     * @param actionNode An instance of ActionNode class.
     */
    public RawLoadActionNode(ActionNode actionNode) {
        setId(actionNode.getId());
        processInfo = actionNode.getProcessInfo();
        this.actionNode = actionNode;
    }

    public ProcessInfo getProcessInfo() {
        return processInfo;
    }


    public String getName() {

        String nodeName = "rawLoad-" + getId() + "-" + processInfo.getProcessName().replace(' ', '_');
        return nodeName.substring(0, Math.min(nodeName.length(), 45));

    }

    @Override
    public String getXML() {
        if (this.getProcessInfo().getParentProcessId() == 0) {
            return "";
        }
        OozieNode fileListNode = null;
        for (OozieNode oozieNode : actionNode.getContainingNodes()) {
            if (oozieNode instanceof LOFActionNode) {
                fileListNode = oozieNode;
            }
        }
        Integer processId = getId();


        StringBuilder ret = new StringBuilder();

        ret.append("\n<action name=\"" + getName() + "\">\n" +
                "        <shell xmlns=\"uri:oozie:shell-action:0.1\">\n" +
                "            <job-tracker>${jobTracker}</job-tracker>\n" +
                "            <name-node>${nameNode}</name-node>\n"+
                "            <exec>raw-load-executor.sh</exec>\n"+
                "            <argument>raw-load.hql</argument>\n"+
                "            <argument>rawtablename="+getRawTableName(getId())+"</argument>\n"+
                "            <argument>rawtableschema="+getRawTableSchema(getId())+ " </argument>\n"+
                "            <argument>lof=${wf:actionData(\"init-job\")[\"file-list-map.FileList."+ getId() +"\"]}</argument>\n"+
                "            <argument>lob=${wf:actionData(\"init-job\")[\"batch-list-map.FileBatchList." + getId() +"\"]} </argument>\n"+
                "            <file>raw-load.hql</file>\n"+
                "            <file>raw-load-executor.sh</file>\n"+
                "            <file>env.sh</file>\n"+
                "        </shell>\n" +
                "        <ok to=\"" + getToNode().getName() + "\"/>\n" +
                "        <error to=\"" + getTermNode().getName() + "\"/>\n" +
                "    </action>");

        return ret.toString();
    }

    public String getRawTableName(Integer processId){
        GetProperties getPropertiesOfRawTable = new GetProperties();
        java.util.Properties rawPropertiesOfTable = getPropertiesOfRawTable.getProperties(processId.toString(), "raw-table");
        String rawTableName = rawPropertiesOfTable.getProperty("table_name");
        String rawTableDbName = rawPropertiesOfTable.getProperty("table_db");
        return rawTableDbName+"."+rawTableName;
    }

    public String getRawTableSchema(Integer processId){
        GetProperties getPropertiesOfRawTable = new GetProperties();
        java.util.Properties rawPropertiesOfTable = getPropertiesOfRawTable.getProperties(processId.toString(), "raw-table");
        String rawTableName = rawPropertiesOfTable.getProperty("table_name");
        String rawTableDbName = rawPropertiesOfTable.getProperty("table_db");
        String rawColumnList = "";

        // fetching column names in a string list from properties with raw-columns as config group
        GetProperties getPropertiesOfRawColumns = new GetProperties();
        java.util.Properties rawPropertiesOfColumns = getPropertiesOfRawColumns.getProperties(processId.toString(), "raw-cols");
        Enumeration columns = rawPropertiesOfColumns.propertyNames();
        List<String> orderOfCloumns = Collections.list(columns);
        Collections.sort(orderOfCloumns);
        List<String> rawColumns = new ArrayList<String>();
        if (!rawPropertiesOfColumns.isEmpty()) {
            for (String columnOrder : orderOfCloumns) {
                String key = columnOrder;
                rawColumns.add(rawPropertiesOfColumns.getProperty(key));
            }
        }

        // fetching column datatypes in a string list from properties with raw-data-types as config group
        GetProperties getPropertiesOfRawDataTypes = new GetProperties();
        java.util.Properties rawPropertiesOfDataTypes = getPropertiesOfRawDataTypes.getProperties(processId.toString(), "raw-data-types");
        Enumeration dataTypes = rawPropertiesOfDataTypes.propertyNames();
        List<String> orderOfDataTypes = Collections.list(dataTypes);
        Collections.sort(orderOfDataTypes);
        List<String> rawDataTypes = new ArrayList<String>();
        if (!rawPropertiesOfColumns.isEmpty()) {
            for (String columnOrder : orderOfDataTypes) {
                String key = columnOrder;
                rawDataTypes.add(rawPropertiesOfDataTypes.getProperty(key));
            }
        }

        // forming a comma separated string in the form of col1 datatype1, col2 datatype2, col3 datatype3 etc.
        for (int i = 0; i < rawColumns.size(); i++) {
            rawColumnList += rawColumns.get(i) + " " + rawDataTypes.get(i) + ",";
        }

        String rawColumnsWithDataTypes = rawColumnList.substring(0, rawColumnList.length() - 1);

        return rawTableDbName + "." + rawTableName + " ( " + rawColumnsWithDataTypes + " ) ";
    }
}