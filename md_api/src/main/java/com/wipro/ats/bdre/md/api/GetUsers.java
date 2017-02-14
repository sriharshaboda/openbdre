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

package com.wipro.ats.bdre.md.api;

import com.wipro.ats.bdre.exception.MetadataException;
import com.wipro.ats.bdre.md.api.base.MetadataAPIBase;
import com.wipro.ats.bdre.md.beans.FileInfo;
import com.wipro.ats.bdre.md.beans.table.Users;
import com.wipro.ats.bdre.md.dao.FileDAO;
import com.wipro.ats.bdre.md.dao.UsersDAO;
import com.wipro.ats.bdre.md.dao.jpa.GeneralConfigId;
import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

import java.util.List;

/**
 * Created by arijit on 12/8/14.
 */
public class GetUsers extends MetadataAPIBase {

    @Autowired
    UsersDAO usersDAO;

    private static final Logger LOGGER = Logger.getLogger(GetUsers.class);
    private static final String[][] PARAMS_STRUCTURE = {
            {"minB", "min-batch-id", "minimum batch id"},
            {"maxB", "max-batch-id", "maximum batch id"}
    };

    public GetUsers() {
        AutowireCapableBeanFactory acbFactory = getAutowireCapableBeanFactory();
        acbFactory.autowireBean(this);
    }
    /**
     * This method runs GetUsers proc for some batch-id between mininmum and maximum batch id
     * and return corresponding file and their server specification.
     *
     * @param params String array containing minimum-batch-id,maximum-batch-id,
     *               env with their respective notation on command line.
     * @return This method return output of GetFile proc having information regarding
     * files and their server specifications.
     */
    @Override
    public Object execute(String[] params) {
        return null;
    }

    public void insertUsers(String username, String password, Boolean enabled) {
        LOGGER.info("inside insert general config");
        try {
            com.wipro.ats.bdre.md.dao.jpa.Users jpaUsers = new com.wipro.ats.bdre.md.dao.jpa.Users();
            jpaUsers.setUsername(username);
            jpaUsers.setPassword(password);
            jpaUsers.setEnabled(enabled);
            usersDAO.insert(jpaUsers);
            LOGGER.info("User inserted= "+jpaUsers.getUsername());
        } catch(Exception ex){
            LOGGER.error("Error occured while inserting into GC "+ ex);
        }
    }
}
