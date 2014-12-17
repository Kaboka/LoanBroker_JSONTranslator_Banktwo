/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bankonetranslator;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import dk.cphbusiness.connection.ConnectionCreator;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Document;
import utilities.xml.xmlMapper;

/**
 *
 * @author Kaboka
 */
public class TranslatorBankTwo {

    private static final String BANKEXCHANGE_NAME = "cphbusiness.bankJSON";
    private static final String EXCHANGE_NAME = "ex_translators_gr1";
    private static final String REPLY_QUEUE = "bank_two_normalizer_gr1";
    private static final String INQUEUE_NAME = "bank_two_translator_gr1";
    private static final String[] TOPICS = {"expensive.*"};

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionCreator creator = ConnectionCreator.getInstance();
        Channel channel = creator.createChannel();
        channel.queueDeclare(INQUEUE_NAME, false, false, false, null);
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        for (String topic : TOPICS) {
            channel.queueBind(INQUEUE_NAME, EXCHANGE_NAME, topic);
        }

        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(INQUEUE_NAME, true, consumer);
        
        System.out.println("Translator for Bank Two running");
        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            System.out.println("Got message: " + new String(delivery.getBody()));
            System.out.println(new String(delivery.getBody()));
            String message = translateMessage(new String(delivery.getBody()));
            BasicProperties probs = new BasicProperties.Builder().replyTo(REPLY_QUEUE).correlationId("1").build(); //change to normalizer queue
            channel.basicPublish(BANKEXCHANGE_NAME, "", probs, message.getBytes());
        }
    }

    private static String translateMessage(String xmlMessage) {
        String jsonString = "";
        try {
            Document doc = xmlMapper.getXMLDocument(xmlMessage);
            XPath xPath = XPathFactory.newInstance().newXPath();
            String ssn = xPath.compile("/LoanRequest/ssn").evaluate(doc);
            String creditScore = xPath.compile("/LoanRequest/creditScore").evaluate(doc);
            String loanAmount = xPath.compile("/LoanRequest/loanAmount").evaluate(doc);
            String loanDuration = xPath.compile("/LoanRequest/loanDuration").evaluate(doc);
            jsonString = "{\"ssn\":" + ssn.replace("-", "") + ","
                    + "\"creditScore\":" + creditScore + ","
                    + "\"loanAmount\":" + loanAmount + ","
                    + "\"loanDuration\":" + loanDuration + "}";
        } catch (XPathExpressionException ex) {
            Logger.getLogger(TranslatorBankTwo.class.getName()).log(Level.SEVERE, null, ex);
        }
        return jsonString;
    }
}
