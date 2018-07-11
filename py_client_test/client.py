# -*- coding: utf-8 -*-
import socket
import json
import struct
import time
import thread


def json_load_byteified(file_handle):
    return _byteify(
        json.load(file_handle, object_hook=_byteify),
        ignore_dicts=True
    )


def json_loads_byteified(json_text):
    return _byteify(
        json.loads(json_text, object_hook=_byteify),
        ignore_dicts=True
    )


def _byteify(data, ignore_dicts=False):
    # if this is a unicode string, return its string representation
    if isinstance(data, unicode):
        return data.encode('utf-8')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [_byteify(item, ignore_dicts=True) for item in data]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }
    # if it's anything else, return it in its original form
    return data


class Message:
    def __init__(self, receiver_id='', sender_id='', sender_ip='', send_time_stamp=0, message_content=''):
        self.Receiver_id = receiver_id
        self.Sender_id = sender_id
        self.Sender_ip = sender_ip
        self.Send_time_stamp = send_time_stamp
        self.Message_content = message_content

    pass


class Auth_info:
    def __init__(self, thisid='', session_guid='', content=''):
        self.Id = thisid
        self.Session_guid = session_guid
        self.Content = content

    pass


class Loginfo:
    def __init__(self, thisid='', password='', time_stamp=0, request='', session_guid=''):
        self.Id = thisid
        self.Password = password
        self.Time_stamp = time_stamp
        self.Request = request
        self.Session_guid = session_guid

    pass


class Reply_info:
    def __init__(self, reply='', messages=[]):
        self.Reply = reply
        self.Messages = messages

    pass


class Request_info:
    def __init__(self, thisid='', request='', session_guid='', messages=[]):
        self.Id = thisid
        self.Request = request
        self.Session_guid = session_guid
        self.Messages = messages

    pass


def dict2message(d):
    return Message(receiver_id=d['Receiver_id'], sender_id=d['Sender_id'], sender_ip=d['Sender_ip'],
                   send_time_stamp=d['Send_time_stamp'], message_content=d['Message_content'])


def dict2auth_info(d):
    return Auth_info(thisid=d['Id'], session_guid=d['Session_guid'], content=d['Content'])


def dict2reply_info(d):
    return Reply_info(reply=d['Reply'], messages=d['Messages'])


def receive_pipe_thread(auth_info_dict=''):

    host_push = '127.0.0.1'
    port_push = 2564
    loginfo = Loginfo(request='relogin', thisid='12345',session_guid=auth_info_dict, time_stamp=int(time.time()))
    loginfo_json = json.dumps(loginfo, default=lambda obj: obj.__dict__)
    pre_length = struct.pack(">I", len(loginfo_json))
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host_push, port_push))
    s.sendall(pre_length)
    s.sendall(loginfo_json)
    lne_bytes = s.recv(4)
    len_auth_info = struct.unpack(">I", lne_bytes)
    json_auth_info = s.recv(len_auth_info[0])
    reply_auth_dict = json_loads_byteified(json_auth_info)
    print(reply_auth_dict)
    if reply_auth_dict['Content'] != 'AUTH':
        return 0
    print('get into thread')
    lne_bytes = s.recv(4)
    len_reply = struct.unpack(">I", lne_bytes)
    json_reply_info = s.recv(len_reply[0])
    reply_info_dict = json_loads_byteified(json_reply_info)
#    if reply_info_dict['Reply'] != 'OK':

    print(reply_info_dict['Messages'])


# login info


loginfo = Loginfo(thisid='12345', password='12345', request='login', time_stamp=int(time.time()))
loginfo_json = json.dumps(loginfo, default=lambda obj: obj.__dict__)
pre_length = struct.pack(">I", len(loginfo_json))

host = '127.0.0.1'
port = 2563

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((host, port))

s.sendall(pre_length)
s.sendall(loginfo_json)

lne_bytes = s.recv(4)
len_auth_info = struct.unpack(">I", lne_bytes)
json_auth_info = s.recv(len_auth_info[0])
# 授权信息
auth_info_dict = json_loads_byteified(json_auth_info)

th=thread.start_new_thread(receive_pipe_thread, (auth_info_dict['Session_guid'],))
time.sleep(2)
# 发送
send_meaasges = []
send_meaasges.append(
    Message(sender_id='12345', receiver_id='12345', send_time_stamp=int(time.time()), message_content='test_message'))

request = Request_info(thisid=auth_info_dict['Id'], request='send_message', messages=send_meaasges)
request_json = json.dumps(request, default=lambda obj: obj.__dict__)

pre_length2 = struct.pack(">I", len(request_json))
s.sendall(pre_length2)
s.sendall(request_json)
#
lne_bytes = s.recv(4)
len_reply = struct.unpack(">I", lne_bytes)
json_reply_info = s.recv(len_reply[0])
reply_info_send_dict = json_loads_byteified(json_reply_info)
print(reply_info_send_dict)
# ------------
#receive_pipe_thread(auth_info_dict=auth_info_dict['Session_guid'])

# 拉取消息-----
request = Request_info(thisid=auth_info_dict['Id'], request='get_message')
request_json = json.dumps(request, default=lambda obj: obj.__dict__)

pre_length2 = struct.pack(">I", len(request_json))
s.sendall(pre_length2)
s.sendall(request_json)
#
lne_bytes = s.recv(4)
len_reply = struct.unpack(">I", lne_bytes)
json_reply_info = s.recv(len_reply[0])
reply_info_get_dict = json_loads_byteified(json_reply_info)

print(reply_info_get_dict)
time.sleep(30)
s.close()
