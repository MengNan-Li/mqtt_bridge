
#! /usr/bin/env python
# -*- coding:utf-8 -*-
import sys
sys.path.append(".")
from MqttBridgeRos import MqttClient


mqtt_client_test = MqttClient()

def on_message(client, userdata, msg):
    print str(msg.payload)

def on_connect(client, userdata, flags, rc):
    print "on_connect"
    mqtt_client_test.mqtt_client.subscribe(
            "topic/terminal_command_pull/robot/" + "WATER_C1S2_00087")
    mqtt_client_test.mqtt_client.subscribe(
        "robot/shuidi/topic/base_data_push")

if __name__ == "__main__":
    mqtt_client_test.init_mqtt_client("WATER-C1S2-00087")

    mqtt_client_test.mqtt_client.on_connect = on_connect
    # mqtt_client_test.mqtt_client.on_disconnect = self.on_disconnect
    mqtt_client_test.mqtt_client.on_message = on_message

    for i in range(5):
        try:
            print "Try to connect to mqtt server"
            mqtt_client_test.mqtt_client.connect(
                "mqtt-lyhsp2t4.bj.mqtt.myqcloud.com", 1883, 60)
        except Exception, e:
            print e
            continue
        break
    mqtt_client_test.mqtt_client.loop_start()
    while True:
        pass