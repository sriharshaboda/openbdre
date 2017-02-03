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

import com.wipro.ats.bdre.md.api.GetProperties;
import com.wipro.ats.bdre.md.beans.ProcessInfo;

import java.util.Enumeration;

/**
 * Created by arijit on 12/21/14.s
 */


/*
Action nodes are the mechanism by which a workflow triggers the execution of a task
Here, we set the id and return name of the action node.
The method getXML() returns a string which contains name, Id, next success node(ToNode) and next failure node(TermNode)
for the current action node, appropriately formatted as XML. 
*/

public class StageLoadActionNode extends GenericActionNode {


    private ProcessInfo processInfo = new ProcessInfo();
    private ActionNode actionNode = null;

    /**
     * This constructor is used to set node id and process information.
     *
     * @param actionNode An instance of ActionNode class.
     */
    public StageLoadActionNode(ActionNode actionNode) {
        setId(actionNode.getId());
        processInfo = actionNode.getProcessInfo();
        this.actionNode = actionNode;
    }

    public ProcessInfo getProcessInfo() {
        return processInfo;
    }


    public String getName() {

        String nodeName = "stageLoad-" + getId() + "-" + processInfo.getProcessName().replace(' ', '_');
        return nodeName.substring(0, Math.min(nodeName.length(), 45));

    }

    @Override
    public String getXML() {
        if (this.getProcessInfo().getParentProcessId() == 0) {
            return "";
        }
        String processId = getId().toString();
        GetProperties getPropertiesOfRawTable = new GetProperties();
        java.util.Properties basePropertiesOfTable = getPropertiesOfRawTable.getProperties(processId, "base-table");
        String baseTableName = basePropertiesOfTable.getProperty("table_name");
        String baseTableDbName = basePropertiesOfTable.getProperty("table_db");
        java.util.Properties rawPropertiesOfTable = getPropertiesOfRawTable.getProperties(processId, "raw-table");
        String rawTableName = rawPropertiesOfTable.getProperty("table_name_raw");
        String rawTableDbName = rawPropertiesOfTable.getProperty("table_db_raw");

        //Getting Stage view information
        String rawViewName = rawTableName + "_view";
        String rawViewDbName = rawTableDbName;
        String rawViewDdl = "";
        String baseTableDdl = "";

        GetProperties getPropertiesOfViewColumns = new GetProperties();
        java.util.Properties viewPropertiesOfColumns = getPropertiesOfViewColumns.getProperties(processId, "base-columns");
        Enumeration viewColumnsList = viewPropertiesOfColumns.propertyNames();
        StringBuilder viewColumns = new StringBuilder();
        if (!viewPropertiesOfColumns.isEmpty()) {
            while (viewColumnsList.hasMoreElements()) {
                String key = (String) viewColumnsList.nextElement();
                viewColumns.append(viewPropertiesOfColumns.getProperty(key) + " AS " + key.replaceAll("transform_", "") + ",");
            }
        }

        java.util.Properties viewPropertiesOfColumnsPartition = getPropertiesOfViewColumns.getProperties(processId, "partition");
        String partitionViewColumn = viewPropertiesOfColumnsPartition.getProperty("partition_columns");
        if (!("".equals(partitionViewColumn)) && !(partitionViewColumn == null)) {
            String[] partitionViewColumns = partitionViewColumn.split(",");
            for (String viewColumn : partitionViewColumns) {
                viewColumns.append(viewColumn.split(" ")[0] + " AS " + viewColumn.split(" ")[0] + ",");
            }
        }

        // adding partition column (additional comma already present at the end of viewColumns
        String viewColumnsWithDataTypes = viewColumns + "batchid";

        // generating stage table ddl
        // fetching column names list
        GetProperties getPropertiesOfBaseColumns = new GetProperties();
        java.util.Properties basePropertiesOfColumns = getPropertiesOfBaseColumns.getProperties(processId, "base-columns");
        java.util.Properties basePropertiesOfDataTypes = getPropertiesOfBaseColumns.getProperties(processId, "base-data-types");
        Enumeration baseColumnsList = basePropertiesOfColumns.propertyNames();
        StringBuilder baseColumns = new StringBuilder();
        if (!basePropertiesOfColumns.isEmpty()) {
            while (baseColumnsList.hasMoreElements()) {
                String key = (String) baseColumnsList.nextElement();
                baseColumns.append(basePropertiesOfColumns.getProperty(key) + " " + basePropertiesOfDataTypes .getProperty(key.replaceAll("transform_","")) + ",");
            }
        }
        //removing trailing comma
        String baseColumnsWithDataTypes = baseColumns.substring(0, baseColumns.length() - 1);
        java.util.Properties partitionproperties = getPropertiesOfRawTable.getProperties(processId, "partition");
        String partitionColumns = partitionproperties.getProperty("partition_columns");
        if (partitionColumns == null)
            partitionColumns = "";

        String fieldNames = getColumnNames(processId);
        String partitionKeys = getPartitionKeys(processId);




        StringBuilder ret = new StringBuilder();
      /*  ret.append("\n<action name=\"" + getName() + "\" cred=\"hs2-creds\" >\n" +
                "        <hive2 xmlns=\"uri:oozie:hive2-action:0.1\">\n" +
                "            <job-tracker>${jobTracker}</job-tracker>\n" +
                "            <name-node>${nameNode}</name-node>\n" +
                "            <job-xml>hive-site.xml</job-xml>\n"+
                "            <jdbc-url>jdbc:hive2://localhost:10000/default</jdbc-url> \n"+
                "            <script>stage-load.hql</script>\n"+

                "            <param>rawViewDbName="+rawViewDbName +"</param>\n"+
                "            <param>rawViewName="+rawViewName +"</param>\n"+
                "            <param>viewColumnsWithDataTypes="+viewColumnsWithDataTypes +"</param>\n"+
                "            <param>rawTableDbName="+rawTableDbName +"</param>\n"+
                "            <param>rawTableName="+rawTableName +"</param>\n"+
                "            <param>baseTableDbName="+baseTableDbName +"</param>\n"+
                "            <param>baseTableName="+baseTableName +"</param>\n"+
                "            <param>baseColumnsWithDataTypes="+baseColumnsWithDataTypes +"</param>\n"+
                "            <param>partitionKeys="+partitionKeys +"</param>\n"+
                "            <param>partitionColumns="+partitionColumns +"</param>\n"+
                "            <param>fieldNames="+fieldNames +"</param>\n"+

                "            <param>instanceExecId=${wf:actionData(\"init-job\")[\"instance-exec-id\"]}</param>\n" +
                "            <param>minBatchId=${wf:actionData(\"init-job\")[\"min-batch-id-map." + getId() + "\"]}</param>\n" +
                "            <param>maxBatchId=${wf:actionData(\"init-job\")[\"max-batch-id-map." + getId() + "\"]}</param>\n" +


                "        </hive2>\n" +
                "        <ok to=\"" + getToNode().getName() + "\"/>\n" +
                "        <error to=\"" + getTermNode().getName() + "\"/>\n" +
                "    </action>");
*/
        ret.append("\n<action name=\"" + getName() + "\">\n" +
                "        <shell xmlns=\"uri:oozie:shell-action:0.1\">\n" +
                "            <job-tracker>${jobTracker}</job-tracker>\n" +
                "            <name-node>${nameNode}</name-node>\n"+
                "            <exec>stage-load-executor.sh</exec>\n"+
                "            <argument>stage-load.hql</argument>\n"+
                "            <argument>rawViewDbName="+rawViewDbName +"</argument>\n"+
                "            <argument>rawViewName="+rawViewName +"</argument>\n"+
                "            <argument>viewColumnsWithDataTypes="+viewColumnsWithDataTypes +"</argument>\n"+
                "            <argument>rawTableDbName="+rawTableDbName +"</argument>\n"+
                "            <argument>rawTableName="+rawTableName +"</argument>\n"+
                "            <argument>baseTableDbName="+baseTableDbName +"</argument>\n"+
                "            <argument>baseTableName="+baseTableName +"</argument>\n"+
                "            <argument>baseColumnsWithDataTypes="+baseColumnsWithDataTypes +"</argument>\n"+
                "            <argument>partitionKeys="+partitionKeys +"</argument>\n"+
                "            <argument>partitionColumns="+partitionColumns +"</argument>\n"+
                "            <argument>fieldNames="+fieldNames +"</argument>\n"+
                "            <argument>instanceExecId=${wf:actionData(\"init-job\")[\"instance-exec-id\"]}</argument>\n" +
                "            <argument>minBatchId=${wf:actionData(\"init-job\")[\"min-batch-id-map." + getId() + "\"]}</argument>\n" +
                "            <argument>maxBatchId=${wf:actionData(\"init-job\")[\"max-batch-id-map." + getId() + "\"]}</argument>\n" +
                "            <file>stage-load.hql</file>\n"+
                "            <file>stage-load-executor.sh</file>\n"+
                "            <file>env.sh</file>\n"+
                "        </shell>\n" +
                "        <ok to=\"" + getToNode().getName() + "\"/>\n" +
                "        <error to=\"" + getTermNode().getName() + "\"/>\n" +
                "    </action>");
        return ret.toString();
    }

    private String getPartitionKeys(String stageLoadProcessId) {

        StringBuilder stringBuilder = new StringBuilder("");
        String result="";
        GetProperties getPropertiesOfRawTable = new GetProperties();
        java.util.Properties partitionproperties = getPropertiesOfRawTable.getProperties(stageLoadProcessId, "partition");
        String partitions = partitionproperties.getProperty("partition_columns");
        if(!("".equals(partitions)) && !(partitions == null)) {
            String[] partitionKeys = partitions.split(",");
            for (int i = 0; i < (partitionKeys.length); i++) {
                stringBuilder.append(partitionKeys[i].split(" ")[0]);
                stringBuilder.append(",");
            }
        }
        result = stringBuilder.toString();
        result=result.substring(0,result.length());
        return result;
    }

    private String getColumnNames(String stageLoadProcessId) {
        GetProperties getPropertiesOfRawTable = new GetProperties();
        String result="";
        StringBuilder columnList = new StringBuilder();
        java.util.Properties columnValues = getPropertiesOfRawTable.getProperties(stageLoadProcessId, "base-columns");
        Enumeration e = columnValues.propertyNames();
        if (!columnValues.isEmpty()) {
            while (e.hasMoreElements()) {
                String key = (String) e.nextElement();
                columnList.append(key.replaceAll("transform_",""));
                columnList.append(",");
            }
            result=columnList.substring(0, columnList.length() - 1);
        }


        return result;
    }
}