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

package com.wipro.ats.bdre.jaas.login;

import com.microsoft.aad.adal4j.AuthenticationResult;
import com.wipro.ats.bdre.md.api.GetGeneralConfig;
import com.wipro.ats.bdre.md.api.GetUserRoles;
import com.wipro.ats.bdre.md.api.GetUsers;
import org.apache.log4j.Logger;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;

/**
 * @author arijit
 */
public class ADJAASLoginModule implements LoginModule {

    private static final Logger LOGGER = Logger.getLogger(ADJAASLoginModule.class);

    // initial state
    private Subject subject;
    private CallbackHandler callbackHandler;
    private Map<String, ?> sharedState;
    private Map<String, ?> options;

    // configurable option

    static String env;
    // the authentication status
    private boolean succeeded = false;
    private boolean commitSucceeded = false;
    private String accessToken = new String();
    private String ADTenant = "ddpus2.onmicrosoft.com";
    //user credentials
    private String username = null;
    private String password = null;

    //user principle
    private JAASUserPrincipal userPrincipal = null;
    private JAASPasswordPrincipal passwordPrincipal = null;

    public ADJAASLoginModule() {
        super();
        LOGGER.info("Login module constructor call");

    }


    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
        this.options = options;
        env = (String) options.get("env");

        LOGGER.info("Login module constructor call. env=" + env);
    }

    @Override
    public boolean login() throws LoginException {

        if (callbackHandler == null) {
            throw new LoginException("Error: no CallbackHandler available " +
                    "to garner authentication information from the user");
        }
        Callback[] callbacks = new Callback[2];
        callbacks[0] = new NameCallback("username");
        callbacks[1] = new PasswordCallback("password: ", false);

        try {

            callbackHandler.handle(callbacks);
            username = ((NameCallback) callbacks[0]).getName();
            password = new String(((PasswordCallback) callbacks[1]).getPassword());

            if (username == null || password == null) {
                LOGGER.error("Callback handler does not return login data properly");
                throw new LoginException("Callback handler does not return login data properly user=" + username);
            }

            if (isValidUser()) { //validate user.
                succeeded = true;
                return true;
            }

        } catch (IOException e) {
            LOGGER.error("Callback handler does not return login data properly. " + e.getMessage());
            LOGGER.info(e);
        } catch (UnsupportedCallbackException e) {
            LOGGER.error("Callback handler does not return login data properly. " + e.getMessage());
            LOGGER.info(e);
        }

        return false;
    }

    @Override
    public boolean commit() throws LoginException {
        if (!succeeded) {
            return false;
        } else {
            userPrincipal = new JAASUserPrincipal(username);
            if (!subject.getPrincipals().contains(userPrincipal)) {
                subject.getPrincipals().add(userPrincipal);
                LOGGER.info("User principal added:" + userPrincipal);
            }
            passwordPrincipal = new JAASPasswordPrincipal(password);
            if (!subject.getPrincipals().contains(passwordPrincipal)) {
                subject.getPrincipals().add(passwordPrincipal);
                LOGGER.info("Password principal added: " + passwordPrincipal);
            }

            //populate subject with roles.
            List<String> roles = getRoles(accessToken);
            LOGGER.info("number of roles fetched ="+roles.size()+ " first role ="+roles.get(0));
            for (String role : roles) {
                JAASRolePrincipal rolePrincipal = new JAASRolePrincipal(role);
                if (!subject.getPrincipals().contains(rolePrincipal)) {
                    LOGGER.info("role to be added in gc = "+role+" username = "+username);
                    GetUserRoles getUserRoles = new GetUserRoles();
                    getUserRoles.insertOrUpdateUserDetails(username,"",true,role);
                    subject.getPrincipals().add(rolePrincipal);
                    LOGGER.info("Role principal added: " + rolePrincipal);
                }
            }
            commitSucceeded = true;

            LOGGER.info("Login subject were successfully populated with principals and roles");

            return true;
        }
    }

    @Override
    public boolean abort() throws LoginException {
        if (!succeeded) {
            return false;
        } else if (succeeded && !commitSucceeded) {
            succeeded = false;
            username = null;
            if (password != null) {
                password = null;
            }
            userPrincipal = null;
        } else {
            logout();
        }
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        subject.getPrincipals().remove(userPrincipal);
        succeeded = false;
        succeeded = commitSucceeded;
        username = null;
        if (password != null) {
            for (int i = 0; i < password.toCharArray().length; i++) {
                password.toCharArray()[i] = ' ';
                password = null;
            }
        }
        userPrincipal = null;
        return true;
    }

    private boolean isValidUser() throws LoginException {
        boolean result=false;
        LOGGER.info("Checking user validity");
        AuthenticationResult aResult = null;
        try {
            aResult = AzureADRealm.getAccessTokenFromUserCredentials(username, password.toString());
            accessToken = aResult.getAccessToken();
            LOGGER.info("Access Token - {0}"+ aResult.getAccessToken());
            LOGGER.info( "Refresh Token - {0}"+ aResult.getRefreshToken());
            LOGGER.info( "ID Token - {0}"+ aResult.getIdToken());
            LOGGER.info( "Login succeeded for username - {0}"+ username);
            result = true;
        } catch (Exception ex) {
            LOGGER.info("Exception message="+ex);
            LOGGER.info( "Login failed for user: - {0}"+ username);
            //  Result keeps initial value which is false
            return result;
        }
        return result;
    }

    /**
     * Returns list of roles assigned to authenticated user.
     *
     * @return
     */
    private List<String> getRoles(String accessToken) {
        List<String> roleList = new ArrayList<String>();
        try{
        String memberId = getMemberId(accessToken, ADTenant);
        Map<String,String> groupIDsFromGeneralConfig = new HashMap<>();
            groupIDsFromGeneralConfig.put("8a56e745-9c63-4356-a91b-9f3ffef7ac18","ROLE_ADMIN");
            groupIDsFromGeneralConfig.put("f3deff5a-ed45-4225-9a89-49efc7ec23f7","ROLE_COADMIN");
            groupIDsFromGeneralConfig.put("033a67f4-f80e-47b4-8474-f993d358ec3d","ROLE_USER");
            groupIDsFromGeneralConfig.put("96ed1673-930e-4837-927b-0a394a4fb297","ROLE_READONLY");

            for(String groupID:groupIDsFromGeneralConfig.keySet()){
                if(isMemberOfGroup(accessToken,ADTenant,memberId,groupID)){
                    roleList.add(groupIDsFromGeneralConfig.get(groupID));
                    break;
                }
            }
        } catch(Exception ex){
            LOGGER.info("Exception encountered while checking group membership "+ ex.getMessage());        }
        return roleList;
    }

    private String getMemberId(String accessToken, String tenant) throws Exception
    {

        URL url = new URL(String.format("https://graph.windows.net/%s/me?api-version=2013-04-05", tenant,
                accessToken));

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        // Set the appropriate header fields in the request header.
        conn.setRequestProperty("api-version", "2013-04-05");
        conn.setRequestProperty("Authorization", accessToken);
        conn.setRequestProperty("Accept", "application/json;odata=minimalmetadata");
        String goodRespStr = HttpClientHelper.getResponseStringFromConn(conn, true);
        int responseCode = conn.getResponseCode();
        JSONObject response = HttpClientHelper.processGoodRespStr(responseCode, goodRespStr);
        String user = response.optJSONObject("responseMsg").getString("objectId");
        System.out.println(response);
        System.out.println(user);
        return user;
    }

    private Boolean isMemberOfGroup(String accessToken, String tenant,String memberID,String groupId) throws Exception
    {
        System.out.println("formatted url is" +String.format("https://graph.windows.net/%s/me/isMemberOf?api-version=2013-04-05", tenant,
                accessToken));
        URL url = new URL(String.format("https://graph.windows.net/%s/isMemberOf?api-version=2013-04-05", tenant,
                accessToken));
        String data = "{\"groupId\":\""+groupId+"\",\"memberId\":\""+memberID+"\"}";
        System.out.println("data = " + data);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        // Set the appropriate header fields in the request header.
        conn.setDoOutput(true);
        conn.setRequestProperty( "Content-Length", Integer.toString(data.length()));
        conn.setRequestProperty( "Content-Type", "application/json");
        conn.setRequestProperty("api-version", "2013-04-05");
        conn.setRequestProperty("Authorization", accessToken);
        conn.setRequestProperty("Accept", "application/json");
        byte[] outputInBytes = data.getBytes("UTF-8");
        OutputStream os = conn.getOutputStream();
        os.write( outputInBytes );
        os.close();
        String goodRespStr = HttpClientHelper.getResponseStringFromConn(conn, true);
        System.out.println("goodRespStr = " + goodRespStr);
        int responseCode = conn.getResponseCode();
        JSONObject goodResponse = HttpClientHelper.processGoodRespStr(responseCode, goodRespStr);
        System.out.println(goodResponse);
        Boolean check = goodResponse.optJSONObject("responseMsg").getBoolean("value");
        return check;
    }

}