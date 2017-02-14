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
import com.wipro.ats.bdre.md.dao.UserRolesDAO;
import com.wipro.ats.bdre.md.dao.UsersDAO;
import com.wipro.ats.bdre.md.dao.jpa.GeneralConfigId;
import com.wipro.ats.bdre.md.dao.jpa.UserRoles;
import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by arijit on 12/8/14.
 */
public class GetUserRoles extends MetadataAPIBase {
    @Autowired
    UsersDAO usersDAO;
    @Autowired
    UserRolesDAO userRolesDAO;

    private static final Logger LOGGER = Logger.getLogger(GetUsers.class);
    private static final String[][] PARAMS_STRUCTURE = {
            {"minB", "min-batch-id", "minimum batch id"},
            {"maxB", "max-batch-id", "maximum batch id"}
    };

    public GetUserRoles() {
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

    public void insertUserRoles(String username, String role) {
        LOGGER.info("inside insert user roles");
        try {
            com.wipro.ats.bdre.md.dao.jpa.UserRoles jpaUserRoles = new com.wipro.ats.bdre.md.dao.jpa.UserRoles();
            com.wipro.ats.bdre.md.dao.jpa.Users users = new com.wipro.ats.bdre.md.dao.jpa.Users();
            users.setUsername(username);
            jpaUserRoles.setUsers(users);
            jpaUserRoles.setRole(role);
            Integer userRolesId = userRolesDAO.insert(jpaUserRoles);
            LOGGER.info("User role "+ jpaUserRoles.getRole()+" inserted for user "+jpaUserRoles.getUsers().getUsername());
        } catch(Exception ex){
            LOGGER.error("Error occured while inserting into GC "+ ex);
        }
    }

    public void updateUserRoles(String username, String role){
        List<com.wipro.ats.bdre.md.dao.jpa.UserRoles> jpaUserRolesList = new ArrayList<>();
        jpaUserRolesList = userRolesDAO.listByName(username);
        for(UserRoles jpaUserRole:jpaUserRolesList){
            jpaUserRole.setUserRoleId(jpaUserRole.getUserRoleId());
            jpaUserRole.setRole(role);
            com.wipro.ats.bdre.md.dao.jpa.Users users = new com.wipro.ats.bdre.md.dao.jpa.Users();
            users.setUsername(username);
            jpaUserRole.setUsers(users);
            userRolesDAO.update(jpaUserRole);
        }

    }

    //this method inserts if userrole does not exist and updates if it already exists
    public void insertOrUpdateUserDetails(String username, String password, Boolean enabled, String role){
        com.wipro.ats.bdre.md.dao.jpa.Users jpaUsers = usersDAO.get(username);
        if(jpaUsers==null){
            GetUsers getUsers = new GetUsers();
            getUsers.insertUsers(username,password,enabled);
            GetUserRoles getUserRoles = new GetUserRoles();
            getUserRoles.insertUserRoles(username,role);
        }
        else{
            GetUserRoles getUserRoles = new GetUserRoles();
            getUserRoles.updateUserRoles(username,role);
        }
    }
}
