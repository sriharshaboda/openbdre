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

import com.wipro.ats.bdre.md.api.base.MetadataAPIBase;
import com.wipro.ats.bdre.md.beans.table.GeneralConfig;
import com.wipro.ats.bdre.md.dao.GeneralConfigDAO;
import com.wipro.ats.bdre.md.dao.jpa.GeneralConfigId;
import org.apache.commons.collections.comparators.BooleanComparator;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by KA294215 on 07-10-2015.
 */


public class GetGeneralConfig extends MetadataAPIBase {

    @Autowired
    GeneralConfigDAO generalConfigDAO;

    private static final Logger LOGGER = Logger.getLogger(GetGeneralConfig.class);

    public GetGeneralConfig() {
        AutowireCapableBeanFactory acbFactory = getAutowireCapableBeanFactory();
        acbFactory.autowireBean(this);
    }

    public List<GeneralConfig> byConigGroupOnly(String configGroup, Integer required) {

        GeneralConfig generalConfig = new GeneralConfig();
        List<GeneralConfig> generalConfigs = new ArrayList<GeneralConfig>();
        try {
            Integer newRequired = required;
            if (required == null) {
                newRequired = 2;
            }
            generalConfigs = generalConfigDAO.getGeneralConfig(configGroup, newRequired);
            LOGGER.info("All records listed with config group" + configGroup);
            LOGGER.info("generalConfigs" + generalConfigs);

        } catch (Exception e) {
            generalConfig.setRequired(2);
            generalConfigs.add(generalConfig);
            LOGGER.error("Listing of Records Failed",e);
        }
        return generalConfigs;
    }
    public List<GeneralConfig>listGeneralConfig(String configGroup) {

        GeneralConfig generalConfig = new GeneralConfig();
        List<GeneralConfig> generalConfigs = new ArrayList<GeneralConfig>();
        try {

            generalConfigs = generalConfigDAO.listGeneralConfig(configGroup);
            LOGGER.info("All records listed with config group" + configGroup);
            LOGGER.info("generalConfigs" + generalConfigs);

        } catch (Exception e) {
            LOGGER.error("Listing of Records Failed",e);
        }
        return generalConfigs;
    }

    public List<GeneralConfig> byLikeConfigGroup(String description, Integer required) {

        GeneralConfig generalConfig = new GeneralConfig();
        List<GeneralConfig> generalConfigs = new ArrayList<GeneralConfig>();
        try {
            Integer newRequired = required;
            if (required == null) {
                newRequired = 2;
            }
            generalConfigs = generalConfigDAO.getLikeGeneralConfig(description, newRequired);
            LOGGER.info("All records listed with config group " + "cluster");
            LOGGER.info("generalConfigs" + generalConfigs);

        } catch (Exception e) {
            generalConfig.setRequired(2);
            generalConfigs.add(generalConfig);
            LOGGER.error("Listing of Records Failed",e);
        }
        return generalConfigs;
    }

    public GeneralConfig byConigGroupAndKey(String configGroup, String key) {

        GeneralConfig generalConfig = new GeneralConfig();
        try {

            generalConfig = generalConfigDAO.getGenConfigProperty(configGroup, key);

            LOGGER.info("Record with config group:" + configGroup + " and key:" + key + "selected from General Config " + generalConfig);

        } catch (Exception e) {
            generalConfig.setRequired(2);
            LOGGER.error("Object with specified config_group and key not found",e);

        }
        return generalConfig;
    }

    public void insertGenConfig(String configGroup, String key, String defaultValue, String desc, Boolean enabled, String gcValue, String type, Boolean required) {
        LOGGER.info("inside insert general config");
        try {
            com.wipro.ats.bdre.md.dao.jpa.GeneralConfig jpaGeneralConfig = new com.wipro.ats.bdre.md.dao.jpa.GeneralConfig();
            GeneralConfigId jpaGeneralConfigId = new GeneralConfigId();
            jpaGeneralConfigId.setConfigGroup(configGroup);
            jpaGeneralConfigId.setGcKey(key);
            jpaGeneralConfig.setDefaultVal(defaultValue);
            jpaGeneralConfig.setDescription(desc);
            jpaGeneralConfig.setEnabled(enabled);
            jpaGeneralConfig.setGcValue(gcValue);
            jpaGeneralConfig.setType(type);
            jpaGeneralConfig.setRequired(required);
            jpaGeneralConfig.setId(jpaGeneralConfigId);
            GeneralConfigId generalConfigId= generalConfigDAO.insert(jpaGeneralConfig);
            LOGGER.info("GC inserted config group= "+generalConfigId.getConfigGroup()+ " key ="+ generalConfigId.getGcKey());
        } catch(Exception ex){
            LOGGER.error("Error occured while inserting into GC "+ ex);
        }
    }

    public void deleteGenConfig(String configGroup, String key){
        try {
            GeneralConfig gc = byConigGroupAndKey(configGroup, key);
            GeneralConfigId jpaGeneralConfigId = new GeneralConfigId();
            jpaGeneralConfigId.setConfigGroup(configGroup);
            jpaGeneralConfigId.setGcKey(key);
            generalConfigDAO.delete(jpaGeneralConfigId);
        } catch(Exception ex){
            LOGGER.error("No object found to delete");
        }
    }
    @Override
    public Object execute(String[] params) {
        return null;
    }
}
