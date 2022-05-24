# -*- coding: utf-8 -*-
"""
Microservice client for MQTT based JSON-RPC smart IOT devices on the edge
Created on Thu Sep 12 09:43:25 2019

@author: mrbluesky125
"""
import paho.mqtt.client as mqtt
import uuid
import time
import json
import re
import os
from datetime import datetime

class MicroserviceClient:
    def __init__(self, serviceName):
        self._serviceName = serviceName
        self._brokerip = os.environ["BROKER_IP"] if "BROKER_IP" in os.environ else ""
        self._clientId = str(uuid.uuid4())
        self._mqttClient = mqtt.Client(self._clientId)
        self._config = {}
        self._serviceDoc = {}
        self._pendingRequests = {}
        self._receivedResults = {}
        self._connected = False
        self._async = False
        self.on_connected = None
        self.on_disconnected = None
        self.on_notification = None
        self.on_binaryNotification = None
        self.on_result = None
        self.on_binaryResult = None
        
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
            self._mqttClient.subscribe("microservice/" + self.serviceName() + "/json/result/#")
            self._mqttClient.subscribe("microservice/" + self.serviceName() + "/binary/result/#")
            self._mqttClient.subscribe("microservice/" + self.serviceName() + "/json/notification/#")
            self._mqttClient.subscribe("microservice/" + self.serviceName() + "/binary/notification/#")
            if self.on_connected is not None:
                self.on_connected()

    def _on_disconnect_handler(self, client, userdata, rc):
        self._connected = False
        print("Disconnected")
        if self.on_disconnected is not None:
            self.on_disconnected()

    def _on_message_handler(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload
        
        regexpResult = re.compile('microservice/(.+)/json/result/(.+)/(.+)')
        regexpBinaryResult = re.compile('microservice/(.+)/binary/result/(.+)/(.+)')
        regexpNotification = re.compile('microservice/(.+)/json/notification/(.+)')
        regexpBinaryNotification = re.compile('microservice/(.+)/binary/notification/(.+)')
        matchResult = regexpResult.search(topic)
        matchBinaryResult = regexpBinaryResult.search(topic)
        matchNotification = regexpNotification.search(topic)
        matchBinaryNotification = regexpBinaryNotification.search(topic)
        
        if matchResult:
            messageObject = json.loads(payload)
            serviceName = matchResult.group(1)
            id = matchResult.group(2)
            clientId = matchResult.group(3)
            if clientId != self.clientId():
                return
            
            print("Result " + id + " from " + serviceName)
            self._on_text_message_handler(id, messageObject.get("result", None))
        elif matchBinaryResult:
            serviceName = matchBinaryResult.group(1)
            id = matchBinaryResult.group(2)
            clientId = matchBinaryResult.group(3)
            if clientId != self.clientId():
                return
            
            print("Result (Binary) " + id + " from " + serviceName)
            self._on_binary_message_handler(id, payload)
        elif matchNotification:
            messageObject = json.loads(payload)
            serviceName = matchNotification.group(1)
            methodName = matchNotification.group(2)
            
            if methodName == 'heartbeat':
                self._on_heartbeat_handler(methodName, messageObject.get('params', None))
                return

            print("Notification " + methodName + " from " + serviceName)            
            if self.on_notification is not None:
                self.on_notification(methodName, messageObject.get('params', None))
        elif matchBinaryNotification:
            serviceName = matchBinaryNotification.group(1)
            methodName = matchBinaryNotification.group(2)

            print("Notification (Binary) " + methodName + " from " + serviceName)            
            if self.on_binaryNotification is not None:
                self.on_binaryNotification(methodName, payload)

    def _on_heartbeat_handler(self, methodName, serviceDoc):
        if methodName != 'heartbeat':
            return
        
        if self._serviceDoc == serviceDoc:
           return 
            
        print("Heartbeat from " + self.serviceName() + " detected!")
        self._serviceDoc = serviceDoc
        
    def _on_text_message_handler(self, id, resultObject):
       
        if self._pendingRequests.get(id, None) is None:
            print("Unknown/timed out reply (" + id + ") received.")
            return
        
        pendingRequests = self._pendingRequests.get(id, None)
        methodName = pendingRequests.get("method", None)
        errorObject = resultObject.get("error", None)
        sync = pendingRequests.get("sync", True)
        del self._pendingRequests[id]
        
        #if sync is true, a call is waiting for the result
        if sync is True:
            self._receivedResults[id] = resultObject;
        
        if errorObject is not None:
            print("Method " + methodName + " returned with an error: ", errorObject) 
            return
            
        if self.on_result is not None:
            self.on_result(methodName, resultObject, id)
            
    def _on_binary_message_handler(self, id, payload):
       
        if self._pendingRequests.get(id, None) is None:
            print("Unknown/timed out reply (" + id + ") received.")
            return
        
        pendingRequests = self._pendingRequests.get(id, None)
        methodName = pendingRequests.get("method", None)
        sync = pendingRequests.get("sync", True)
        del self._pendingRequests[id]
        
        #if sync is true, a call is waiting for the result
        if sync is True:
            self._receivedResults[id] = payload;
            
        if self.on_binaryResult is not None:
            self.on_binaryResult(methodName, payload, id)

    def brokerip(self):
        return self._brokerip

    def clientId(self):
        return self._clientId

    def serviceName(self):
        return self._serviceName

    def serviceDoc(self):
        return self.serviceDoc

    def config(self):
        return self._config
     
    def callMethod(self, methodName, params, timeout = 1.):
        id = str(uuid.uuid4())
        messageObject = {
                "jsonrpc": "2.0",
                "method": methodName,
                "params": params,
                "id": id
                }
        
        internalMessageObject = messageObject
        internalMessageObject["sent"] = datetime.now().isoformat();
        internalMessageObject["timeout"] = timeout;
        internalMessageObject["sync"] = not self._async;
        self._pendingRequests[id] = internalMessageObject
        
        payload = json.dumps(messageObject)
        topic = "microservice/" + self._serviceName + "/json/call/" + methodName + "/" + self._clientId
        self._mqttClient.publish(topic, payload)
        
        print("Call sent to", topic)
        if self._async is True:
            return id
        
        start_time=time.time()
        while True:
            receivedResult = self._receivedResults.get(id, None)
            if receivedResult is not None:
                del self._receivedResults[id]
                return receivedResult

            elapsed_time=time.time()-start_time
            if elapsed_time > timeout:
                return None;
            
            time.sleep(0.10)
            
    def callBinaryMethod(self, methodName, params, timeout = 1.):
        messageId = str(uuid.uuid4())
        
        internalMessageObject = {}
        internalMessageObject["sent"] = datetime.now().isoformat();
        internalMessageObject["timeout"] = timeout;
        internalMessageObject["sync"] = not self._async;
        self._pendingRequests[messageId] = internalMessageObject
        
        payload = params
        topic = "microservice/" + self._serviceName + "/binary/call/" + methodName + "/" + self._clientId + "/" + messageId
        self._mqttClient.publish(topic, payload)
        
        print("Call sent to", topic)
        if self._async is True:
            return messageId
        
        start_time=time.time()
        while True:
            receivedResult = self._receivedResults.get(messageId, None)
            if receivedResult is not None:
                del self._receivedResults[messageId]
                return receivedResult

            elapsed_time=time.time()-start_time
            if elapsed_time > timeout:
                return None;
            
            time.sleep(0.10)

    def notify(self, methodName, params):  
        messageObject = {
                "jsonrpc": "2.0",
                "method": methodName,
                "params": params
              }
        
        payload = json.dumps(messageObject)
        topic = "microservice/" + self._serviceName + "/json/notify/" + methodName + "/" + self._clientId
        self._mqttClient.publish(topic, payload)
        
        print("Notify sent to", topic)
        
    def binaryNotify(self, methodName, payload):  
        topic = "microservice/" + self._serviceName + "/binary/notify/" + methodName + "/" + self._clientId
        self._mqttClient.publish(topic, payload)
        
        print("Binary notify sent to", topic)

    def start(self):
        if not self._brokerip:
            print("No broker set!")
        print("Connecting to MQTT-broker", self._brokerip, "...")
        self._mqttClient.connect(self._brokerip)
        self._mqttClient.loop_start()
      
    def stop(self):
        self._mqttClient.disconnect()
        self._mqttClient.loop_stop()




