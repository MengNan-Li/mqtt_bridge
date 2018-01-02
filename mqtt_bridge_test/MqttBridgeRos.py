#! /usr/bin/env python
# -*- coding:utf-8 -*-
'''
The MqttBridgeRos class composed of the RosTransition class and the class MqttClient.
(1)RosTransition
(2)MqttClient:
订阅云端一个topic，然后通过RosTransition发布到WaterMqttConvertMain，该模块先判断robotid是不是自己，然后根据data.type进行对应的处理
发布云端两个topic，一个机器人状态，一个反馈(就是云端下发的move的反馈)

Defect：
MqttClient logging using rospy's log.
RosTransition‘s tipic_callback using mqtt_client
'''

# pip install paho-mqtt
import rospy
import json
import time
import random
import os
import sys
import paho.mqtt.client as mqtt
from mqtt_bridge.srv import *
from mqtt_bridge_test.msg import *
from std_msgs.msg import *
#import demjson
try:
    import thread
except ImportError:  # TODO use Threading instead of _thread in python3
    import _thread as thread


def json_loads_byteified(json_text):
    return _byteify(json.loads(json_text, object_hook=_byteify), ignore_dicts=True)


def _byteify(data, ignore_dicts=False):
    if isinstance(data, unicode):
        return data.encode('utf-8')
    if isinstance(data, list):
        return [_byteify(item, ignore_dicts=True) for item in data]
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }
    return data


class MqttBridgeRos():
    def __init__(self, *args, **kwargs):
        self.mqttClient = MqttClient()
        self.rosTransition = RosTransition()

    def start(self):
        if self.rosTransition.start():
            self.mqttClient.init_mqtt_client(self.rosTransition.robotid)
            self.rosTransition.set_mqtt_client(self.mqttClient.mqtt_client)
            self.mqttClient.mqtt_client.on_connect = self.on_connect
            self.mqttClient.mqtt_client.on_disconnect = self.on_disconnect
            self.mqttClient.mqtt_client.on_message = self.on_message
            while not rospy.is_shutdown():
                try:
                    rospy.loginfo("Try to connect to mqtt server")
                    self.mqttClient.mqtt_client.connect(
                        "mqtt-lyhsp2t4.bj.mqtt.myqcloud.com", 1883, 60)
                except Exception, e:
                    time.sleep(2)
                    print e
                    rospy.logwarn("Failed to connect to mqtt server")
                    continue
                break
            self.mqttClient.mqtt_client.loop_start()
            rospy.on_shutdown(self.mqttClient.mqtt_client.disconnect)
            rospy.on_shutdown(self.mqttClient.mqtt_client.loop_stop)
            return True
        else:
            return False

    def on_connect(self, client, userdata, flags, rc):
        if self.mqttClient.connect_init_flag:
            rospy.loginfo("Reconnected with result code " + str(rc))
            self.rosTransition.launch_pub.publish("reconnect")
        else:
            rospy.loginfo("Connected with result code " + str(rc))
            self.mqttClient.connect_init_flag = True
            self.rosTransition.on_mqtt_connection()
        robotId_ = self.rosTransition.robotid.split('-')[0] + "_"+self.rosTransition.robotid.split('-')[1] + "_"+ self.rosTransition.robotid.split('-')[2] + "_"
        self.mqttClient.mqtt_client.subscribe(
            "topic/terminal_command_pull/robot/" + robotId_)

    def on_disconnect(self, client, userdata, rc):
        self.rosTransition.launch_pub.publish("disconnect")
        rospy.loginfo('MQTT disconnected')

    def on_message(self, client, userdata, msg):
        rospy.loginfo(msg.topic + " " + str(msg.payload))
        m = Mqtt_test()
        # need productID == my robot's id
        msg_dic = json_loads_byteified(str(msg.payload))
        print msg_dic
        m.robotid = self.rosTransition.robotid
        m.type = str(msg_dic['type'])
        m.data = json.dumps(msg_dic['data'])
        self.rosTransition.pub_mqtt_go_down.publish(m)


class RosTransition():
    def __init__(self):
        rospy.init_node('mqtt_ros_transition')

    def start(self):
        try:
            self.robotid = rospy.get_param('/water_global/product_id')
        except:
            rospy.logerr("Failed to get product_id.")
            return False
        if self.robotid == "":
            rospy.logwarn("product_id is empty")
            return False
        self.pub_mqtt_go_down = rospy.Publisher(
            '/mqtt_bridge_ros/mqtt_topic_go_down', Mqtt_test, queue_size=10)
        self.launch_pub = rospy.Publisher(
            '/mqtt_bridge_ros/connect_status_test', String, queue_size=1, latch=True)
        return True

    def set_mqtt_client(self, mqtt_client):
        self.mqtt_client = mqtt_client

    def on_mqtt_connection(self):
        rospy.Service('/mqtt_bridge_ros/mqtt_service',
                      MqttService, self.handle_service)
        rospy.Subscriber("/mqtt_bridge_ros/mqtt_topic_go_up", Mqtt_test,
                         self.topic_callback, queue_size=10)
        time.sleep(1)
        self.launch_pub.publish("connect")

    def handle_service(self, req):
        # TODO
        return MqttBrigeServiceResponse(self.result, self.message)

    def topic_callback(self, data):
        # rospy.loginfo(data)
        if data.type == 'status':
            topic = "robot/shuidi/topic/base_data_push"
            mqtt_data = {"type": data.type,
                         "data": json_loads_byteified(data.data)}
            self.mqtt_client.publish(topic, json.dumps(mqtt_data))
        if data.type == 'callback':
            topic = "robot/terminal_command_pull_cb"
            #return_dic = {"type":data.type, "data":demjson.decode(data.data)}
            return_dic = {"type": data.type,
                          "data": json_loads_byteified(data.data)}
            print 'mqtt publish callback:' + str(return_dic)
            self.mqtt_client.publish(topic, str(return_dic))


class MqttClient():
    def __init__(self):
        self.connect_init_flag = False
        self.sub_list = []

    def start(self):
        self.init_mqtt_client()
        return True

    def init_mqtt_client(self, robotid):
        endpoint = 'mqtt://mqtt-lyhsp2t4.bj.mqtt.myqcloud.com:1883'
        clientId = 'mqtt-lyhsp2t4@productId'
        client_head = 'mqtt-lyhsp2t4@'
        client_id = client_head + robotid
        print client_id
        username = 'AKID0vfDdrQ81iCUaTuye8VgfhXAbrxqqL5l'
        password = 'TKvtCLaXUSMAxn+vSIpYL/uV4u7IqMJmJM/n2TocbY0='
        self.mqtt_client = mqtt.Client(client_id)
        self.mqtt_client.username_pw_set(username, password)


if __name__ == '__main__':
    mqttBridgeRos = MqttBridgeRos()
    if mqttBridgeRos.start():
        rospy.loginfo("MqttBridgeRos is started.")
        rospy.spin()
    else:
        rospy.logerr("Failed to start MqttBridgeRos.")
