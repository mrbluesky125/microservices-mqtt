# -*- coding: utf-8 -*-
"""
Microservice server for MQTT based JSON-RPC smart IOT devices on the edge
Created on Mon Aug  9 11:35:48 2021

@author: mrbluesky125
"""
import paho.mqtt.client as mqtt
import uuid
import time
import json
import re
import os
import threading
from datetime import datetime

class Microservice:
    def __init__(self, serviceName):
        self._serviceName = serviceName
        self._brokerip = os.environ["BROKER_IP"] if "BROKER_IP" in os.environ else ""
        self._type = "unknown"
        self._mqttClient = mqtt.Client(serviceName)
        self._config = {}
        self._prodcedureMap = {}
        self._binaryProdcedureMap = {}
        self._connected = False
        self._async = False
        self.on_notify = None
        self.on_binaryNotify = None
        self.on_call = None
        self.on_binaryCall = None
        
        self._mqttClient.on_connect = self._on_connect_handler
        self._mqttClient.on_disconnect = self._on_disconnect_handler
        self._mqttClient.on_message = self._on_message_handler
        
        self._mqttClient.username_pw_set("None", "None")

    def __enter__(self):
        if not self._brokerip:
            print("No broker set!")
        print("Connecting to MQTT-broker", self._brokerip, "...")
        self._mqttClient.connect(self._brokerip)
        self._mqttClient.loop_start()
        while self._connected is False:
            time.sleep(0.1)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._mqttClient.disconnect();
        while self._connected is True:
            time.sleep(0.1)
        self._mqttClient.loop_stop()

    def _on_connect_handler(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT-broker, subscribing to service topics...")
            self._connected = True
            self._mqttClient.subscribe("microservice/" + self.serviceName() + "/json/call/#")
            self._mqttClient.subscribe("microservice/" + self.serviceName() + "/binary/call/#")
            self._mqttClient.subscribe("microservice/" + self.serviceName() + "/json/notify/#")
            self._mqttClient.subscribe("microservice/" + self.serviceName() + "/binary/notify/#")
            self.sendHeartbeat()

    def _on_disconnect_handler(self, client, userdata, rc):
        self._connected = False
        print("Disconnected")

    def _on_message_handler(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload
        
        regexpCall = re.compile('microservice/(.+)/json/call/(.+)/(.+)')
        regexpBinaryCall= re.compile('microservice/(.+)/binary/call/(.+)/(.+)/(.+)')
        regexpNotify = re.compile('microservice/(.+)/json/notify/(.+)/(.+)')
        regexpBinaryNotify = re.compile('microservice/(.+)/binary/notify/(.+)/(.+)')
        matchCall = regexpCall.search(topic)
        matchBinaryCall = regexpBinaryCall.search(topic)
        matchNotify = regexpNotify.search(topic)
        matchBinaryNotify = regexpBinaryNotify.search(topic)
        
        if matchCall:
            messageObject = json.loads(payload)
            serviceName = matchCall.group(1)
            methodName = matchCall.group(2)
            clientId = matchCall.group(3)
                        
            print("Call " + methodName + " from " + clientId + " (old-style)")
            self._on_text_message_handler(messageObject, methodName, clientId)
        elif matchBinaryCall:
            serviceName = matchBinaryCall.group(1)
            methodName = matchBinaryCall.group(2)
            clientId = matchBinaryCall.group(3)
            messageId = matchBinaryCall.group(4)
            
            print("Call (Binary) " + methodName + " from " + clientId + " (old-style)")
            self._on_binary_message_handler(payload, methodName, clientId, messageId)
       
    def _on_text_message_handler(self, messageObject, methodName, clientId):
 
        jsonrpc = messageObject.get("jsonrpc", None)
        method = messageObject.get("method", None)
        params = messageObject.get("params", None)
        messageId = messageObject.get("id", None)

        callback = self._prodcedureMap[methodName]
        result = callback(params)
        
        resultObject = {
                "jsonrpc": "2.0",
                "method": methodName,
                "result": result
              }
        payload = json.dumps(resultObject)
        self._sendResult("json", messageId, clientId, payload)
            
    def _on_binary_message_handler(self, payload, methodName, clientId, messageId):
        
        callback = self._binaryProdcedureMap[methodName]
        result = callback(payload)
        self._sendResult("binary", messageId, clientId, result)
            
    def _sendResult(self, protocol, messageId, clientId, payload):
        topic = "microservice/" + self._serviceName + "/" + protocol + "/result/" + messageId + "/" + clientId
        self._mqttClient.publish(topic, payload)

    def brokerip(self):
        return self._brokerip

    def serviceType(self):
        return self._type

    def serviceName(self):
        return self._serviceName

    def config(self):
        return self._config
            
    def registerMethod(self, methodName, callback):
        self._prodcedureMap[methodName] = callback

    def registerBinaryMethod(self, methodName, callback):
        self._binaryProdcedureMap[methodName] = callback

    def unregister(self, methodName):
        del self._prodcedureMap[methodName]
        del self._binaryProdcedureMap[methodName]

    def sendHeartbeat(self):
        serviceDocument = {
                "rpcMethods": [],
                "rpcMethods_binary": [],
                "type": self.serviceType(),
                "config": self.config()
              }
        
        self.sendNotification("heartbeat", serviceDocument)
        if self._connected is True:
            threading.Timer(1, self.sendHeartbeat).start()

    def sendNotification(self, methodName, params):  
        messageObject = {
                "jsonrpc": "2.0",
                "method": methodName,
                "params": params
              }
        
        payload = json.dumps(messageObject)
        topic = "microservice/" + self._serviceName + "/json/notification/" + methodName
        self._mqttClient.publish(topic, payload)
        
        if methodName != "heartbeat":
            print("Notification sent to", topic)
        
    def sendBinaryNotification(self, methodName, payload):  
        topic = "microservice/" + self._serviceName + "/binary/notification/" + methodName
        self._mqttClient.publish(topic, payload)
        
        print("Binary notification sent to", topic)

    def start(self):
        if not self._brokerip:
            print("No broker set!")
        print("Connecting to MQTT-broker", self._brokerip, "...")
        self._mqttClient.connect(self._brokerip)
        self._mqttClient.loop_start()
      
    def stop(self):
        self._mqttClient.disconnect()
        self._mqttClient.loop_stop()






