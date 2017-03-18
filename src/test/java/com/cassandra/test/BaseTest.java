package com.cassandra.test;


import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.springboot.Application;





@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public abstract class BaseTest {
    private long startTime;
    
    private long endTime;
    @Before
    public void before(){
    	 startTime = System.currentTimeMillis();
    }

    @After
    public void after(){
    	endTime = System.currentTimeMillis();
        System.out.println(" 本次测试总用时  :" + (endTime - startTime) + " 毫秒 ");
    }

}