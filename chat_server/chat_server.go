package main

import(
	"fmt"
	"net"
	"log"
//	"io"
	"bufio"
//	"time"
//	"strconv"
	"encoding/json"
	"encoding/binary"
	"bytes"
//	"time"
//	"reflect"
	"github.com/garyburd/redigo/redis"
	"crypto/md5"  
    "crypto/rand"  
	"encoding/base64" 
	"encoding/hex"  
	"io"
	
//	"reflect"
//	"bytes"
)
type message struct{
	receiver_id string
	sender_id string
	sender_ip string
	send_time_stamp int64
	message_content string
}

type auth_info struct{
	id string
	session_guid string
	content string
}

type login_info struct{
	id string
	password string
	time_stamp string
	request string
	log_chan chan auth_info
}

type reply_info struct{
	reply string
	messages []message
}

type request_info struct{
	request string
	session_guid string
	messages []message
	reply_chan chan reply_info
}




type session struct {
tcp_connection net.Conn
conn_time_stamp int64
send_channel  chan message
}

type User_Table map[string]session


func main(){
	var chan_buffer_size=50
	user_table:=make(User_Table)

	//身份验证过程
	login_process_chan:=make(chan login_info,chan_buffer_size)
	//请求处理过程
	request_process_chan:=make(chan request_info,chan_buffer_size)
	
	addr:="127.0.0.1:2563"
	listener,err:=net.Listen("tcp",addr)
	if err != nil {
	   log.Fatal(err)
	    }
	defer listener.Close()
	for {
		conn,err:=listener.Accept()
		if err!=nil{
			log.Fatal(err)
		}
	//	timestamp := time.Now().Unix()

		go handle_conn(conn,&user_table,request_process_chan,login_process_chan)
	}
}

func handle_conn(conn net.Conn,user_table * User_Table,request_process_chan chan request_info,login_process_chan chan login_info){
	
	content_buff,err:=read_content(conn)
	if err!=nil{
		conn.Close()
		return 
	}
	//解析json内容
	var user_info =make(map[string]string)
	
	err=json.Unmarshal(content_buff, &user_info)
	if err!=nil{
		conn.Close()
		return
	}
	fmt.Println(user_info)
	
	//检查权限
	recv_log := login_info{}
    err = json.Unmarshal(content_buff, &recv_log)
    if err != nil {

        log.Println(err)
        return
	}
	//发送登录信息到登录处理过程
	recv_log.log_chan=make(chan auth_info)
	login_process_chan<-recv_log
	//获得返回结果
	auth_message:=<-recv_log.log_chan
	//结果送回socket另一端
	json_string,_:=json.Marshal(auth_message)
	send_string:=write_content(json_string)
	_, err = conn.Write([]byte(send_string))
	if err!=nil{
		conn.Close()
		return
	}
	//检查权限情况(该链接被授予guid权限)
	if auth_message.content!="AUTH"{
		conn.Close()
		return
		
	}
	//获取链接的标示符
	session_guid:=auth_message.session_guid

//循环处理请求
for {

	request_data:=request_info{}
	replay_pipe:=make(chan reply_info)
	//读取请求
	content_buff,err=read_content(conn)
	if err!=nil{
		conn.Close()
		break 
	}

    err := json.Unmarshal(content_buff, &request_data)
    if err != nil {
		conn.Close()
        log.Println(err)
        break
	}
	request_data.reply_chan=replay_pipe
	request_data.session_guid=session_guid//标示链接的客户端,guid将被授予id权限
	//请求送入处理过程
	request_process_chan<-request_data
	//获取处理结果,发送数据
	reply_content:=<-request_data.reply_chan

		json_string,_:=json.Marshal(reply_content)
		send_string:=write_content(json_string)
		_, err = conn.Write([]byte(send_string))
		if err!=nil{
			conn.Close()
			break
		}
		//检查这个会话是否依然有效,过期或者登录顶替(检查guid的id权限是否依旧有效)
		if reply_content.reply=="TIMEOUT"||reply_content.reply=="OCCUPIED"{
		conn.Close()
		break
		}
		
}

}


func login_process(login_process_chan chan login_info){
	//链接redis服务器
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
    if err != nil {
        fmt.Println("Connect to redis error", err)
        return
    }
    defer c.Close()

	log_reply_message:=new(auth_info)

	for {
		login_info:=<-login_process_chan
		if login_info.request=="login" {
			//登录处理
			returned_password,err:=redis.String(c.Do("HGET","user_auth",login_info.id))
			if login_info.password==returned_password&&err!=nil{
				log_reply_message.content="AUTH"
				log_reply_message.session_guid=UniqueId(login_info.id)
				//检测是否已经登录
				n,err:=redis.String(c.Do("HGET", "user_guid",login_info.id))
				if n!=""&&err==nil {
				//解除guid--id映射(解除上一个guid的该id权限)
				_,err=c.Do("HDEL", "guid_id",n)
				}
				//替换id----guid映射,guid---id映射(允许路由,允许id权限)
				_,err=redis.Bool(c.Do("HSET", "user_guid",login_info.id,log_reply_message.session_guid))
				_,err=redis.Bool(c.Do("HSET", "guid_id",log_reply_message.session_guid,login_info.id))	
			//....
			}else {log_reply_message.content="UNEXCEPTECED ERROR"}

		}else if login_info.request=="regist" {
			//注册处理
			is_usable, err := redis.Bool(c.Do("HSETNX", "user_auth",login_info.id,login_info.password))
			if err == nil&&is_usable==true{log_reply_message.content="AUTH"
			//guid处理
			log_reply_message.session_guid=UniqueId(login_info.id)
			_,err=c.Do("HSET", "user_guid",login_info.id,log_reply_message.session_guid)
			_,err=c.Do("HSET", "guid_id",log_reply_message.session_guid,login_info.id)
					
			}else {log_reply_message.content="UNEXCEPTECED ERROR"}
    		//....

		}else {
			//default处理
			log_reply_message.content="UNRECOGNIZED REQUEST"
			//返回结果
			login_info.log_chan<-*log_reply_message
		}


	}
}

//请求处理(检查guid是否依旧有id权限,将消息照表送入相应的guid bucket,检查guid bucket内的消息,并返回请求)
func request_process(request_process_chan chan request_info){

}

//tcp 切流
func read_content(conn net.Conn) (content_buff []byte,err error) {
	reader:=bufio.NewReader(conn)

	length_buff:=make([]byte,4)
	var content_length int32
	bytesBuffer := bytes.NewBuffer(length_buff) 
	var total_reader int=0
	temp_slice:=length_buff[0:4]
//获取内容长度
	for {		
		n,err:=reader.Read(temp_slice)	
		if err!=nil{
			conn.Close()
			return nil,err
		}
		total_reader+=n
		if total_reader<4{
			temp_slice=length_buff[total_reader:3]
		}else{break}
		
	}
	binary.Read(bytesBuffer, binary.BigEndian, &content_length)

	content_buff=make([]byte,content_length)
	temp_slice=content_buff[0:content_length]
	//读取内容
	for {		
		n,err:=reader.Read(temp_slice)	
		if err!=nil{
			conn.Close()
			return nil,err
		}
		total_reader+=n
		if total_reader<int(content_length){
			temp_slice=content_buff[total_reader:content_length]
		}else{break}
		
	}
	return content_buff,err
}

func write_content(json_string []byte ) (send_string []byte ){

	length:=int32(len(json_string))
	length_Buffer := bytes.NewBuffer([]byte{})  
	binary.Write(length_Buffer, binary.BigEndian, length) 

	send_string=append(length_Buffer.Bytes(),json_string...)
	return send_string
	//tongbuc
}

//生成32位md5字串  
func GetMd5String(s string) string {  
    h := md5.New()  
    h.Write([]byte(s))  
    return hex.EncodeToString(h.Sum(nil))  
}  
  
//生成Guid字串  
func UniqueId(s string) string {  
    b := make([]byte, 8)  
    
    if _, err := io.ReadFull(rand.Reader, b); err != nil {  
        return ""  
    } 
     s+=string(b)
     fmt.Println(s)
    return GetMd5String(base64.URLEncoding.EncodeToString(b))  
} 