#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''
处理MqttBridgeRos下发的ros topic，2s上发一次robot status
'''

import rospy
import json
import time
import random
import os
import sys
import commands
import subprocess
from rospy_message_converter import json_message_converter
from geometry_msgs.msg import *
from std_msgs.msg import *
from water_msgs.msg import *
from water_msgs.srv import *
from mqtt_bridge_test.msg import *
try:
    import thread
except ImportError:  # TODO use Threading instead of _thread in python3
    import _thread as thread
import urllib2
reload(sys)
sys.setdefaultencoding("utf-8")


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


class WaterMqttConverterMain():
    def __init__(self):
        self.mqtt_bridge_connect = False
        self.robot_status = {}

    def start(self):
        try:
            self.robotid = rospy.get_param('/water_global/product_id')
            self.robot_status['no'] = self.robotid
        except:
            rospy.logerr("Failed to get product_id")
            return False
        if self.robotid == "":
            rospy.logwarn("product_id is empty")
            return False
        rospy.Subscriber("/mqtt_bridge_ros/mqtt_topic_go_down",
                         Mqtt_test, self.mqtt_topic_go_down_callback)
        rospy.Subscriber("/mqtt_bridge_ros/connect_status_test",
                         String, self.mqtt_bridge_connect_status_callback)
        self.pub_mqtt_go_up = rospy.Publisher(
            '/mqtt_bridge_ros/mqtt_topic_go_up', Mqtt_test, queue_size=10, latch=True)
        return True

    def init_robot_status(self):
        # get software version
        software_version = ''
        srv = rospy.ServiceProxy("/bash_services/software", CmdService)
        req = CmdServiceRequest()
        req.cmd = "get_version"
        try:
            rsp = srv(req)
            if rsp.success:
                software_version = rsp.message
        except Exception as e:
            rospy.logerr("get software version failed, exception:%s" % e)
        self.robot_status['sdk'] = software_version

        # hardware version check
        '''
        check_result = ''
        srv = rospy.ServiceProxy("/bash_services/hardware", CmdService)
        req = CmdServiceRequest()
        req.cmd = "check_for_update"
        try:
            rsp = srv(req)
            if rsp.success:
                j = json.loads(rsp.message)
                for key in sorted(j.keys()):
                    check_result += 'x' if j[key]['enable_update'] else 'o'
        except Exception as e:
            rospy.logerr("get software version failed, exception:%s" % e)
        self.robot_status['hardwareCheck'] = check_result
        '''

        # get lan wifi
        lan_ip = ''
        srv = rospy.ServiceProxy("/bash_services/wifi", CmdService)
        req = CmdServiceRequest()
        req.cmd = "info"
        try:
            rsp = srv(req)
            if rsp.success:
                j = json.loads(rsp.message)
                lan_ip = j['IPaddr']
        except Exception as e:
            rospy.logerr("get software version failed, exception:%s" % e)
        #self.robot_status['lanIp'] = lan_ip
        if lan_ip != '':
            self.robot_status["outernet"] = 1
        else:
            self.robot_status["outernet"] = 0

        '''
        # get map name
        map_name = rospy.get_param("/water_global/hotel_id", "")
        self.robot_status['currentMap'] = map_name
        '''
        # dummy, wait robot status callback
        self.robot_status['floor'] = 0
        self.robot_status['power'] = 0
        # self.robot_status['runningState'] = 'error'
        self.robot_status['softwareStop'] = False
        self.robot_status['hardwareStop'] = False
        self.robot_status['internet'] = 0  # 1:normal,2:not normal, 0:unkonwn
        self.robot_status['notes'] = " "
        self.robot_status['currentPosition'] = {"name": "", "x": 0, "y": 0}
        self.robot_status['angle'] = 0

        # pub robot base info
        rospy.Subscriber("/robot_status", RobotStatus,
                         self.robot_status_callback)
        time.sleep(2)
        while not rospy.is_shutdown():
            if self.mqtt_bridge_connect:
                m = Mqtt_test()
                m.type = 'status'
                m.robotid = self.robotid
                m.data = json.dumps(self.robot_status)
                self.pub_mqtt_go_up.publish(m)
                # rospy.loginfo(m)
            time.sleep(2)

    def robot_status_callback(self, data):
        self.robot_status['floor'] = data.current_floor
        self.robot_status['power'] = data.power_percent
        self.robot_status['softwareStop'] = data.estop_state
        if data.charge_state:
            self.robot_status['charged'] = True
        else:
            self.robot_status['charged'] = False
        if data.move_status == 'running':
            self.robot_status['workingStatus'] = 2
        else:
            self.robot_status['workingStatus'] = 1

        '''
        if data.move_status == 'running':
            self.robot_status['runningState'] = '任务中'
        elif data.charge_state:
            self.robot_status['runningState'] = '充电中'
        elif data.move_status == 'idle':
            self.robot_status['runningState'] = '空闲'
        elif data.move_status == 'failed':
            self.robot_status['runningState'] = '任务失败'
        elif data.move_status == 'succeeded':
            self.robot_status['runningState'] = '任务成功'
        elif data.move_status == 'canceled':
            self.robot_status['runningState'] = '任务取消'
        '''

    def mqtt_bridge_connect_status_callback(self, data):
        rospy.loginfo("mqtt_bridge %s", data.data)
        if data.data == "connect":
            self.mqtt_bridge_connect = True
            thread.start_new_thread(self.init_robot_status, ())
        elif data.data == "reconnect":
            self.mqtt_bridge_connect = True
        elif data.data == "disconnect":
            self.mqtt_bridge_connect = False

    def mqtt_topic_go_down_callback(self, data):
        # rospy.loginfo(data)
        if data.robotid != self.robotid:
            # rospy.logerr("mismatched robotid(%s<=>%s)", self.robotid, data.robotid)
            return

        if data.type == 'move':
            #rospy.INFO("mqtt go down--move:"+data.data)
            print data.data
            # TODO
            # 从话题中获得机器人状态依赖发送的频率，肯定有延迟，应该用service
            if(self.robot_status['workingStatus'] == 2):
                # robot is running
                return_data = Mqtt_test()
                return_data.type = "callback"
                data_dict = {"cb": mqtt_data['cb'],
                             "status": "20002", "msg": "working"}
                return_data.data = json.dumps(data_dict)
                self.pub_mqtt_go_up.publish(return_data)
            else:
                mqtt_data = json_loads_byteified(data.data)
                urllib2.urlopen(
                    "http://localhost:8899/api/move?marker=" + mqtt_data["position"])
                return_data = Mqtt_test()
                return_data.type = "callback"
                data_dict = {"cb": mqtt_data['cb'],
                             "status": "20001", "msg": "success"}
                return_data.data = json.dumps(data_dict)
                self.pub_mqtt_go_up.publish(return_data)

        if data.type == 'heart':
            rospy.INFO("mqtt go down--heart:" + data.data)
        if data.type == 'call':
            rospy.INFO("mqtt go down--call:" + data.data)
        if data.type == 'charge':
            rospy.INFO("mqtt go down--charge:" + data.data)
        if data.type == 'update':
            rospy.INFO("mqtt go down--update:" + data.data)
        if data.type == 'status':
            rospy.INFO("mqtt go down--status:" + data.data)

    def handle_map_notice(self, data):
        map_srv = rospy.ServiceProxy("/bash_services/map", CmdService)
        req = CmdServiceRequest()
        req.cmd = "download_notice"
        req.message = data
        try:
            rsp = map_srv(req)
        except Exception as e:
            rospy.logerr("call map service failed, exception:%s" % e)


if __name__ == '__main__':
    rospy.init_node('water_mqtt_converter_test')
    hc = WaterMqttConverterMain()
    if hc.start():
        rospy.loginfo("mqtt_converter is started.")
        rospy.spin()
    else:
        rospy.logerr("Failed to start mqtt_converter.")
