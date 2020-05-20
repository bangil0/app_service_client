/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.micro.msb.client;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;

public abstract class MqttConnector extends Thread implements MqttCallback {

    private String url;
    private String username;
    private String password;
    
    private MqttClient client;
    private MqttConnectOptions connOpt;

    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    public MqttConnector() {
    }

    protected String generateId() {
        final String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        final int N = alphabet.length();
        Random r = new Random();
        StringBuilder str = new StringBuilder();
        str = str.append(String.valueOf((char) (r.nextInt(26) + 'A')).toUpperCase());
        for (int i = 0; i < 5; i++) {
            str = str.append(alphabet.charAt(r.nextInt(N)));
        }
        return str.toString();
    }

    public String formatDate() {
        return dateFormat.format(new Date());
    }

    public void reconnect() {
        connect(url,username,password);
    }
    
    public void connect(String url,String username,String password) {
        this.url = url;
        this.username = username;
        this.password = password;
        try {
            connOpt = new MqttConnectOptions();
            connOpt.setCleanSession(true);
            connOpt.setKeepAliveInterval(30);
            if(username!=null)
                connOpt.setUserName(username);
            if(password!=null)
                connOpt.setPassword(password.toCharArray());

            client = new MqttClient(url,generateId());
            client.connect(connOpt);
            client.setCallback(this);

        } catch (MqttException me) {
            System.out.println("MqttException(connect):"+me.getMessage());
            try{
                Thread.sleep(5000);
            }catch(Exception e){e.printStackTrace();}
            reconnect();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean isConnected() {
        if (client==null) return false;
        return client.isConnected();
    }

    public void subscribeTopic(String topic) {
        try {
            client.subscribe(topic);

        } catch (MqttException me) {
            System.out.println("MqttException:"+me.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void publishTopic(String topic, String payload) {
        try {
            MqttTopic iot = client.getTopic(topic);
            MqttMessage message = new MqttMessage();
            message.setPayload(payload.getBytes());
            MqttDeliveryToken token = iot.publish(message);
            token.waitForCompletion();

        } catch (MqttException me) {
            System.out.println("MqttException:"+me.getMessage());
            reconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("connectionLost:"+cause.getMessage());
        reconnect();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        System.out.println("deliveryComplete:"+token.toString());
    }
}
