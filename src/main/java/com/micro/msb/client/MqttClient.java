/*
 * 
 * Created : Yan Yan Purdiansah @2020
 * 
 */
package com.micro.msb.client;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.List;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;

public class MqttClient extends MqttConnector {

    private static final String MQTT_SERVER_URL = "tcp://mqtt.eclipse.org:1883";    
    private final String serverKey = "wmQ45QjYpC38W3i";
    private final String userKey = "wmQ45QjYpC38W3i";
    private final Gson gson = new Gson();
    
    public MqttClient() {
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        System.out.println("-------------------------------------------------");
        System.out.println("| Topic     :" + topic);
        System.out.println("| Message Id:" + Integer.toString(message.getId()));
        System.out.println("-------------------------------------------------");
        
        String messageEncrypted = new String(message.getPayload());
        String messageDecrypted = AuthFactory.decryptPassword(messageEncrypted,serverKey);
        
        Payload payloadIn = gson.fromJson(messageDecrypted, Payload.class);
        String from = payloadIn.getFrom();
        String to = payloadIn.getTo();
        String subject = payloadIn.getSubject();
        
        System.out.println("From:"+from);
        System.out.println("To:"+to);
        System.out.println("Subject:"+subject);
        
        if(subject!=null){
            String bodyDecrypted = AuthFactory.decryptPassword(payloadIn.getBody(),userKey);
            System.out.println("->"+bodyDecrypted);
        }
        else{
            String bodyDecrypted = AuthFactory.decryptPassword(payloadIn.getBody(),userKey);
            List<String> listServices = gson.fromJson(bodyDecrypted,new TypeToken<List<String>>(){}.getType());
            listServices.forEach((service) -> {
                System.out.println(service);
            });
        }
    }

    @Override
    public void run() {
        connect(MQTT_SERVER_URL,"user01","1234");        
        while(true){
            try{
                JSONObject input = new JSONObject();
                input.put("rowid",0);
                String body = input.toString();
                String bodyEncrypted = AuthFactory.encryptPassword(body,userKey);
                
                Payload payload = new Payload();
                payload.setTo("node01@domain.com");
                payload.setFrom("user01@domain.com");
                payload.setSubject("/searchAppUsers");
                payload.setBody(bodyEncrypted);
                                
                String message = gson.toJson(payload);
                String messageEncrypted = AuthFactory.encryptPassword(message,serverKey);
                subscribeTopic(payload.getFrom());
                publishTopic(payload.getTo(),messageEncrypted);
                
                Thread.sleep(10000);
                
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        MqttClient mqttClient = new MqttClient();
        mqttClient.start();
    }
    
    public class Payload {
        
        private String to;
        private String from;
        private String subject;
        private String body;

        public String getTo() {
            return to;
        }

        public void setTo(String to) {
            this.to = to;
        }

        public String getFrom() {
            return from;
        }

        public void setFrom(String from) {
            this.from = from;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }
    }
}
