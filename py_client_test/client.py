import socket
import json


class Message:
    def __init__(self, receiver_id='', sender_id='', sender_ip='', send_time_stamp='', message_content=''):
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
    def __init__(self, thisid='', password='', time_stamp='', request='', session_guid=''):
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
    return Message(d['Receiver_id'], d['Sender_id'], d['Sender_ip'], d['Send_time_stamp'], d['Message_content'])


loginfo = Loginfo(thisid='12345', password='12345', request='login')
loginfo_json = json.dumps(loginfo, default=lambda obj: obj.__dict__)
print(loginfo_json)

host = '127.0.0.1'
port = 2563
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


s.connect((host, port))

s.sendall(loginfo_json)
data = s.recv(1024)
print(data)
s.close()
