package com.test.jms.bpmclient;

import org.junit.Test;
import org.kie.remote.client.jaxb.JaxbCommandsRequest;
import org.kie.services.client.serialization.JaxbSerializationProvider;
import org.kie.remote.client.jaxb.ClientJaxbSerializationProvider;
import org.kie.services.client.serialization.SerializationConstants;
import org.kie.remote.jaxb.gen.StartProcessCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.UUID;
import javax.jms.Message;

public class JmsClient  {
    private static final Logger logger = LoggerFactory.getLogger(JmsClient.class);

    // kie server users, admin rules is needed by jms provider
    private static final String BPM_USER = "tao";
    private static final String BPM_PASSWORD = "password1!";

    private static final String PROVIDER_URL = "http-remoting://127.0.0.1:8080";
    private static final String JMS_CONNECTION_FACTORY = "jms/RemoteConnectionFactory";
    private static final String JMS_QUEUE_KIE_SESSION = "jms/queue/KIE.SESSION";
    private static final String JMS_QUEUE_KIE_RESPONSE = "jms/queue/KIE.RESPONSE";

    private static final String DEPLOYMENT_ID = "com.test:bpmtest:1.3-SNAPSHOT";
    private static final String PROCESS_ID = "testproject.hello-process";

    @Test
    public void startProcess() throws NamingException, Exception {

        InitialContext ctx = initContext();

        logger.info("create connection factory... "+JMS_CONNECTION_FACTORY);
        ConnectionFactory conn = (ConnectionFactory) ctx.lookup(JMS_CONNECTION_FACTORY);

        logger.info("look up queue..." + JMS_QUEUE_KIE_SESSION);
        Queue reqQueue = (Queue) ctx.lookup(JMS_QUEUE_KIE_SESSION);

        JaxbCommandsRequest req = createCmdRequest();

        sendJmsCommands(DEPLOYMENT_ID, BPM_USER, req, conn, reqQueue, BPM_USER, BPM_PASSWORD, 5);
    }

    @Test
    public void getStartProcessResponse() throws NamingException, Exception {

        InitialContext ctx = initContext();

        logger.info("create connection factory... "+JMS_CONNECTION_FACTORY);
        ConnectionFactory conn = (ConnectionFactory) ctx.lookup(JMS_CONNECTION_FACTORY);

        logger.info("look up queue..." + JMS_QUEUE_KIE_RESPONSE);
        Queue responseQueue = (Queue) ctx.lookup(JMS_QUEUE_KIE_RESPONSE);

        Connection connection = conn.createConnection(BPM_USER, BPM_PASSWORD);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageConsumer consumer = session.createConsumer(responseQueue);
        connection.start();

        logger.info("Received message with content");
        Message msg = consumer.receiveNoWait();
        while(msg != null){
            TextMessage message = (TextMessage)msg;
            logger.info(message.getText());

            msg = consumer.receiveNoWait();
        }

        consumer.close();
        session.close();
        connection.close();
    }




    private static void sendJmsCommands(String deploymentId, String user,
                                 JaxbCommandsRequest commandsRequest, ConnectionFactory connectionFactory,
                                 Queue sendQueue, String jmsUser, String jmsPassword, int timeout)
            throws Exception {

        commandsRequest.setUser(user);

        String corrId = UUID.randomUUID().toString();

        Connection connection = connectionFactory.createConnection(jmsUser, jmsPassword);
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(sendQueue);
        connection.start();
        JaxbSerializationProvider serializationProvider = ClientJaxbSerializationProvider.newInstance();

        TextMessage msg = session.createTextMessage();
        msg.setJMSCorrelationID(corrId);
        msg.setStringProperty(SerializationConstants.DEPLOYMENT_ID_PROPERTY_NAME, deploymentId);
        msg.setIntProperty(SerializationConstants.SERIALIZATION_TYPE_PROPERTY_NAME,JaxbSerializationProvider.JMS_SERIALIZATION_TYPE);
        msg.setText(serializationProvider.serialize(commandsRequest));

        System.out.println("Request to be send: " + serializationProvider.serialize(commandsRequest));
        producer.send(msg);

        producer.close();
        session.close();
        connection.close();
    }

    public static InitialContext initContext() throws NamingException{
        java.util.Properties env = new java.util.Properties();
        env.put(javax.naming.Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
        env.put(javax.naming.Context.PROVIDER_URL, PROVIDER_URL);
        env.put(javax.naming.Context.SECURITY_PRINCIPAL, BPM_USER);
        env.put(javax.naming.Context.SECURITY_CREDENTIALS, BPM_PASSWORD);
        InitialContext ctx = new InitialContext(env);

        return ctx;
    }

    public static JaxbCommandsRequest createCmdRequest(){
        logger.info("create start process command");
        StartProcessCommand cmd = new StartProcessCommand();
        cmd.setProcessId(PROCESS_ID);
        JaxbCommandsRequest req = new JaxbCommandsRequest(DEPLOYMENT_ID, cmd);
        return req;
    }

}