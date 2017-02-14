package com.wipro.ats.bdre.md.api;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by cloudera on 2/14/17.
 */
public class GetUserRolesTest {
    @Ignore
    @Test
    public void testInsertUserRoles() throws Exception {

        GetUserRoles getUserRolesTest = new GetUserRoles();
        getUserRolesTest.insertUserRoles("testuser","testrole");

    }
    @Ignore
    @Test
    public void testUpdateUserRoles() throws Exception {

        GetUserRoles getUserRolesTest = new GetUserRoles();
        getUserRolesTest.updateUserRoles("testuser","updatedrole");

    }

    @Test
    public void testInsertOrUpdateUserRoles() throws Exception {

        GetUserRoles getUserRolesTest = new GetUserRoles();
        getUserRolesTest.insertOrUpdateUserDetails("abcuser","",false,"ROLE_ADMIN");
    }
}