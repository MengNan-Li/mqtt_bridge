#! /usr/bin/env python

# pip install paho-mqtt
import rospy
import json
import time
import random
import os, sys
import paho.mqtt.client as mqtt
from mqtt_bridge.srv import *
from mqtt_bridge_test.msg import *
from std_msgs.msg import *
try:
    import thread
except ImportError:  # TODO use Threading instead of _thread in python3
    import _thread as thread

reload(sys)
sys.setdefaultencoding( "utf-8" )

class MqttBridgeMain():
    def __init__(self):
        self.connect_init_flag = False;
	self.sub_list = []

    def start(self):
        try:
            self.robotid = rospy.get_param('/water_global/product_id')
        except:
            rospy.logerr("Failed to get product_id.")
            return False
        if self.robotid == "":
            rospy.logwarn("product_id is empty")
            return False
        self.mqtt_topic_pub = rospy.Publisher('/mqtt_bridge/mqtt_topic', Mqtt_test, queue_size=10)
        self.launch_pub = rospy.Publisher('/mqtt_bridge/connect_status', String, queue_size=1, latch=True)
        self.init_mqtt_client()
        return True

    def init_mqtt_client(self):
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.username_pw_set(self.robotid)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_message = self.on_message
        while True:
            try:
                rospy.loginfo("Try to connect to mqtt server")
                self.mqtt_client.connect("mqtt.yunjichina.com.cn", 1883, 60)
            except Exception,e:
                time.sleep(2)
                rospy.logwarn("Failed to connect to mqtt server")
                continue
            break
        #self.mqtt_client.connect("218.80.198.62", 1883, 60)
        self.mqtt_client.loop_start()
        rospy.on_shutdown(self.mqtt_client.disconnect)
        rospy.on_shutdown(self.mqtt_client.loop_stop)

    def on_connect(self, client, userdata, flags, rc):
        if self.connect_init_flag:
            rospy.loginfo("Reconnected with result code "+str(rc))
	    for t in self.sub_list:
                self.mqtt_client.subscribe(t)
            self.launch_pub.publish("reconnect")
        else:
	    rospy.loginfo("Connected with result code "+str(rc))
            rospy.Service('/mqtt_bridge/mqtt_service', MqttService, self.handle_service)
            rospy.Subscriber("/mqtt_bridge/mqtt_pub", Mqtt, self.topic_callback, queue_size=10)
	    self.connect_init_flag = True
            time.sleep(1)
            self.launch_pub.publish("connect")

    def on_disconnect(self, client, userdata, rc):
        self.launch_pub.publish("disconnect")
        rospy.loginfo('MQTT disconnected')

    def on_message(self, client, userdata, msg):
        #rospy.loginfo(msg.topic+" " + str(msg.payload))
        m = Mqtt()
        m.name = msg.topic.split('/')[3]
        m.robotid = msg.topic.split('/')[1]
        m.type = msg.topic.split('/')[2]
        m.data = str(msg.payload)
        self.mqtt_topic_pub.publish(m)

    def handle_service(self, req):
        # TODO
        return MqttBrigeServiceResponse(self.result, self.message)

    def topic_callback(self, data):
        # rospy.loginfo(data)
        if data.type == 'pub':
            if data.name == "robot_base_info":
                topic = 'robot/water/'
            else:
                topic = 'robot/'
            topic +=  self.robotid + '/topic/' + data.name
            self.mqtt_client.publish(topic, data.data)
            #rospy.loginfo('pub mqtt topic: %s', topic)
        elif data.type == 'sub':
            topic = 'robot/' + data.robotid + '/topic/' + data.name
            if topic not in self.sub_list:
                self.sub_list.append(topic)
                self.mqtt_client.subscribe(topic)
                rospy.loginfo("sub mqtt topic:%s", topic) 
        elif data.type == 'unsub':
            topic = 'robot/' + data.robotid + '/topic/' + data.name
            if topic in self.sub_list:
                self.sub_list.remove(topic)
                self.mqtt_client.unsubscribe(topic)
                rospy.loginfo("unsub mqtt topic:%s", topic) 
        elif data.type == 'req':
            self.mqtt_client.subscribe('robot/' + data.robotid + '/resp/' + data.name)
            self.mqtt_client.publish('robot/' + data.robotid + '/req/' + data.name, data.data)
        elif data.type == 'resp':
            self.mqtt_client.publish('robot/' + self.robotid + '/resp/' + data.name, data.data)

if __name__ == '__main__':
    rospy.init_node('mqtt_bridge')
    hc = MqttBridgeMain()
    if hc.start():
        rospy.loginfo("mqtt_bridge is started.")
        rospy.spin()
    else:
        rospy.logerr("Failed to start mqtt_bridge.")
