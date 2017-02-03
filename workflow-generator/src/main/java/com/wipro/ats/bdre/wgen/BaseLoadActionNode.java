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

/**
 * Created by arijit on 12/21/14.s
 */


/*
Action nodes are the mechanism by which a workflow triggers the execution of a task
Here, we set the id and return name of the action node.
The method getXML() returns a string which contains name, Id, next success node(ToNode) and next failure node(TermNode)
for the current action node, appropriately formatted as XML.
*/

public class BaseLoadActionNode extends GenericActionNode {


    private ProcessInfo processInfo = new ProcessInfo();
    private ActionNode actionNode = null;

    /**
     * This constructor is used to set node id and process information.
     *
     * @param actionNode An instance of ActionNode class.
     */
    public BaseLoadActionNode(ActionNode actionNode) {
        setId(actionNode.getId());
        processInfo = actionNode.getProcessInfo();
        this.actionNode = actionNode;
    }

    public ProcessInfo getProcessInfo() {
        return processInfo;
    }


    public String getName() {

        String nodeName = "baseLoad-" + getId() + "-" + processInfo.getProcessName().replace(' ', '_');
        return nodeName.substring(0, Math.min(nodeName.length(), 45));

    }

    @Override
    public String getXML() {
        if (this.getProcessInfo().getParentProcessId() == 0) {
            return "";
        }

        GetProperties getPropertiesOfBaseTable = new GetProperties();
        java.util.Properties basePropertiesOfTable = getPropertiesOfBaseTable.getProperties(getId().toString(), "base-table");
        String baseTable = basePropertiesOfTable.getProperty("table_name");
        String baseDb = basePropertiesOfTable.getProperty("table_db");



        StringBuilder ret = new StringBuilder();
       /* ret.append("<action name=\"" + getName() + "\">\n" +
                "        <hive2 xmlns=\"uri:oozie:hive2-action:0.1\" cred=\"hs2-creds\">\n" +
                "            <job-tracker>${jobTracker}</job-tracker>\n" +
                "            <name-node>${nameNode}</name-node>\n" +
                "            <job-xml>hive-site.xml</job-xml>\n"+
                "            <jdbc-url>jdbc:hive2://localhost:10000/default</jdbc-url> \n"+
                "            <script>base-load.hql</script>\n"+
                "             <param>baseDb="+baseDb+"</param>\n"+
                "             <param>baseTable="+baseTable+"</param>\n"+

                "            <param>instanceExecId=${wf:actionData(\"init-job\")[\"instance-exec-id\"]}</param>\n" +


                "        </hive2>\n" +
                "        <ok to=\"" + getToNode().getName() + "\"/>\n" +
                "        <error to=\"" + getTermNode().getName() + "\"/>\n" +
                "    </action>");
*/
        ret.append("\n<action name=\"" + getName() + "\">\n" +
                "        <shell xmlns=\"uri:oozie:shell-action:0.1\">\n" +
                "            <job-tracker>${jobTracker}</job-tracker>\n" +
                "            <name-node>${nameNode}</name-node>\n"+
                "            <exec>base-load-executor.sh</exec>\n"+
                "            <argument>base-load.hql</argument>\n"+
                "            <argument>baseDb="+baseDb+"</argument>\n"+
                "            <argument>baseTable="+baseTable+"</argument>\n"+
                "            <argument>instanceExecId=${wf:actionData(\"init-job\")[\"instance-exec-id\"]}</argument>\n" +
                "            <file>base-load.hql</file>\n"+
                "            <file>base-load-executor.sh</file>\n"+
                "            <file>env.sh</file>\n"+
                "        </shell>\n" +
                "        <ok to=\"" + getToNode().getName() + "\"/>\n" +
                "        <error to=\"" + getTermNode().getName() + "\"/>\n" +
                "    </action>");
        return ret.toString();
    }
}