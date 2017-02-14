package com.wipro.ats.bdre.md.api;

import com.wipro.ats.bdre.md.dao.jpa.Users;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by cloudera on 2/14/17.
 */
public class GetUsersTest {

    @Test
    public void testInsertUsers() throws Exception {

        GetUsers getUsers = new GetUsers();
        getUsers.insertUsers("testuser","",true);


    }
}