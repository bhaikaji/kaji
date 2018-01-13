package com.kaji;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.FileInputStream;
import java.io.StringWriter;

public class ReadXML {

    private static final String DEFAULT_CONNECTION_URI = "tcp://jms.unitx.com:61616?jms.watchTopicAdvisories=false";
    private static final String QUEUE_NAME = "VesselMetrics";
    private static final String TEXT = "";
    
    private Connection createConnection(){
        return this.createConnection(DEFAULT_CONNECTION_URI);
    }

    
    private Connection createConnection(String uri){
        ConnectionFactory factory = new ActiveMQConnectionFactory(uri);
        Connection connection = null;
        try {
            connection = factory.createConnection();
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return connection;
    }
    
    private Session createSession(Connection connection){
        Session session = null;
        try {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return session;
    }
    
    private Destination createDestination(Session session){
        Destination destination = null;
        try {
            destination = session.createQueue(QUEUE_NAME);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return destination;
    }
    
    private MessageProducer createMessageProducer(Session session, Destination destination){
        MessageProducer producer = null;
        try {
            producer = session.createProducer(destination);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        return producer;
    }
    
    public void write(String text){
        Connection connection = createConnection();
        Session session = createSession(connection);
        Destination destination = createDestination(session);
        MessageProducer producer = createMessageProducer(session, destination);
        Message message = null;
        try {
            if(text != null){
                message = session.createTextMessage(text);
            } else{
                message = session.createTextMessage(TEXT);
            }
            producer.send(message);
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static Document parseXml(String xmlFile) throws Exception {
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory
                .newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory
                .newDocumentBuilder();
        return documentBuilder.parse(new FileInputStream(xmlFile));
    }

    public static String getXmlAsString(Document document) throws Exception {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter writer = new StringWriter();
        transformer
                .transform(new DOMSource(document), new StreamResult(writer));
        String output = writer.getBuffer().toString().replaceAll("\n|\r", "");
        return output;
    }

    public static void sendXML(String fileName, ReadXML test) throws Exception{

        Document doc = parseXml(fileName);
        String xmlString = getXmlAsString(doc);
        test.write(xmlString);

    }


    public static void main(String[] args) throws Exception{
        ReadXML readXML = new ReadXML();

        sendXML("profile.xml", readXML);
        sendXML("InitializeMetrics.xml", readXML);

        for(int i = 0; i < 16771; i++ ) {
            String fileName = String.format("%05d", i);
            String fullPath = "Messages" + "/" + fileName + ".xml";
            Document doc = parseXml(fullPath);
            System.out.println(fileName + ".xml");

            String xmlString = getXmlAsString(doc);
            System.out.println(xmlString);
            System.out.println();
            System.out.println();
            readXML.write(xmlString);
        }
    }
    
}
