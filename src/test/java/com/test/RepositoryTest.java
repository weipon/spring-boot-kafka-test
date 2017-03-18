package com.test;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;


public class RepositoryTest extends BaseTest {
    private static final String TEMPLATE_TOPIC = "templateTopic";

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;
    /**---dev/test**/
    @Test
    public void Test() {


    }


}
