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
	"time"
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
	Receiver_id string `json:"Receiver_id"`
	Sender_id string `json:"Sender_id"`
	Sender_ip string `json:"Sender_ip"`
	Send_time_stamp int64 `json:"Send_time_stamp"`
	Message_content string `json:"Message_content"`
}

type auth_info struct{
	Id string `json:"Id"`
	Session_guid string `json:"Session_guid"`
	Content string `json:"Content"`
}

type login_info struct{
	Id string `json:"Id"`
	Password string `json:"Password"`
	Time_stamp int64 `json:"Time_stamp"`
	Request string `json:"Request"`
	Session_guid string `json:"Session_guid"`
	log_chan chan auth_info 
}

type reply_info struct{
	Reply string `json:"Reply"`
	Messages []string `json:"Messages"`
}

type request_info struct{
	Id string `json:"Id"`
	Request string `json:"Request"`
	Session_guid string `json:"Session_guid"`
	Messages []message `json:"Messages"`
	reply_chan chan reply_info
}

type push_chan struct{
	chan_buff_size int
	Messages_chan chan reply_info
}

type id_guid_pair struct{
	Id string
	Guid string
}

func main(){
	var chan_buffer_size=4
	//guid--->channel
	push_conn_table:=make(map[string]*push_chan)
	//身份验证
	login_process_chan:=make(chan login_info,chan_buffer_size)
	//请求处理
	request_process_chan:=make(chan request_info,chan_buffer_size)
	id_guid_chan:=make(chan id_guid_pair,chan_buffer_size)
	//回收
	recollect_process_chan:=make(chan []string,chan_buffer_size)
	kick_off_list_chan:=make(chan string,chan_buffer_size)
	//启动所有后端过程
	i:=1
	for i<chan_buffer_size {
		i++
		go login_process(login_process_chan,kick_off_list_chan) //登录
		go request_process(request_process_chan,id_guid_chan)//请求处理
		go push_backen_process(id_guid_chan ,&push_conn_table)
		go recollect_process(recollect_process_chan)//回收发送失败的消息
	}
	go kick_off_process(kick_off_list_chan ,&push_conn_table)
	
	//----推送端口监听
	addr_push:="127.0.0.1:2564"
	listener_push,err:=net.Listen("tcp",addr_push)
	if err != nil {
	   log.Fatal(err)
	    }
	defer listener_push.Close()

	go func(){
		conn_push,err:=listener_push.Accept()
		if err!=nil{
			log.Fatal(err)
		}
		 handle_push(conn_push,login_process_chan,&push_conn_table,recollect_process_chan )
	}()
	//....主端口监听----------
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
		go handle_conn(conn,request_process_chan,login_process_chan,recollect_process_chan)
	}
	
}

func handle_conn(conn net.Conn,request_process_chan chan request_info,login_process_chan chan login_info,recollect_process_chan chan []string){
	
	content_buff,err:=read_content(conn)
	if err!=nil{
		conn.Close()
		return 
	}
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
	if auth_message.Content!="AUTH"{
		conn.Close()
		return
		
	}


	request_data:=request_info{}
	replay_pipe:=make(chan reply_info)
//循环处理请求
	for {
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

		request_data.Session_guid=auth_message.Session_guid//标示链接的客户端,guid将被授予id权限
		request_data.Id=auth_message.Id

		//请求送入处理过程
		request_process_chan<-request_data
		//获取处理结果,发送数据
		reply_content:=<-request_data.reply_chan

			json_string,_:=json.Marshal(reply_content)
			send_string:=write_content(json_string)
			_, err = conn.Write([]byte(send_string))
		
			if err!=nil{
				conn.Close()
				if len(reply_content.Messages)>0{
					recollect_process_chan<-reply_content.Messages
				}
				break
			}
			//检查这个会话是否依然有效,错误或者登录顶替(检查guid的id权限是否依旧有效)
			if reply_content.Reply=="ERROR"||reply_content.Reply=="OCCUPIED"{
				conn.Close() 
				break}
			
				
	}

}

func handle_push(conn net.Conn,login_process_chan chan login_info,push_conn_table * map[string]*push_chan,recollect_process_chan chan []string){
	content_buff,err:=read_content(conn)
	if err!=nil{
		conn.Close()
		return 
	}
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
	if auth_message.Content!="AUTH"{
		conn.Close()
		return
		
	}

	push_channel:=new(push_chan)
	push_channel.chan_buff_size=20
	push_channel.Messages_chan=make(chan reply_info,push_channel.chan_buff_size)
	(*push_conn_table)[recv_log.Session_guid]=push_channel

	for {
		Messages:=<-push_channel.Messages_chan
		json_string,err:=json.Marshal(Messages)
		if err!=nil{
			log.Println(err)
			continue
		}
		send_string:=write_content(json_string)
		_, err = conn.Write([]byte(send_string))
		if err!=nil{
			conn.Close()
			recollect_process_chan<-Messages.Messages
			break
		}
		if Messages.Reply!="OK"{
			conn.Close()
			break
		}

	}
	delete((*push_conn_table),recv_log.Session_guid)	
}

func push_backen_process(id_guid_chan chan id_guid_pair,push_conn_table * map[string]*push_chan){
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
    if err != nil {
        fmt.Println("Connect to redis error", err)
        return
    }
	defer c.Close()
	reply_content:=reply_info{}
	for {

		id_guid:=<-id_guid_chan
		push_channel,ok:=(*push_conn_table)[id_guid.Guid]
		if ok{
			//阻塞状态的连接直接放弃推送
			if len(push_channel.Messages_chan)<push_channel.chan_buff_size{
				number,err:=redis.Int(c.Do("SCARD",id_guid.Id))
				reply_content.Messages,err=redis.Strings(c.Do("SPOP",id_guid.Id,number))
				if err!=nil {
					reply_content.Reply="OK"
				}else { 
					reply_content.Reply="ERROR"
					}

				push_channel.Messages_chan<-reply_content	
				}						
		}
	}

}

func kick_off_process(kick_off_list_chan chan string,push_conn_table * map[string]*push_chan){
	kick_off_notice:=reply_info{}
	kick_off_notice.Reply="OCCUPIED"
	for {
		kick_off_guid:=<-kick_off_list_chan
		_,ok:=(*push_conn_table)[kick_off_guid]
		if ok {
			if  len((*push_conn_table)[kick_off_guid].Messages_chan)<(*push_conn_table)[kick_off_guid].chan_buff_size {

			(*push_conn_table)[kick_off_guid].Messages_chan<-kick_off_notice
		}else {
			kick_off_list_chan<-kick_off_guid
		}
		}
		
	}
	
}

func login_process(login_process_chan chan login_info,kick_off_list_chan chan string){
	//链接redis服务器
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
    if err != nil {
        fmt.Println("Connect to redis error", err)
        return
    }
    defer c.Close()

	

	for {
		log_reply_message:=new(auth_info)
		login_info:=<-login_process_chan
		//登录处理
		if login_info.Request=="login" {
			returned_password,_:=redis.String(c.Do("HGET","user_auth",login_info.Id))
			if login_info.Password==returned_password{
				log_reply_message.Content="AUTH"
				log_reply_message.Session_guid=UniqueId(login_info.Id)
				log_reply_message.Id=login_info.Id
				//检测是否已经登录
				n,err:=redis.String(c.Do("HGET", "id_guid",login_info.Id))
				if n!=""&&err==nil {
				//解除guid--id映射(解除上一个guid的该id权限)
					_,err=c.Do("HDEL", "guid_id",n)
				//推送下线要求通知
					kick_off_list_chan<-n
				}
				//替换id----guid映射,guid---id映射(允许路由,允许id权限)
				_,err=redis.Bool(c.Do("HSET", "id_guid",login_info.Id,log_reply_message.Session_guid))
				_,err=redis.Bool(c.Do("HSET", "guid_id",log_reply_message.Session_guid,login_info.Id))	
				//....
			}else {log_reply_message.Content="UNEXCEPTECED ERROR"}
		//重新登录处理
		}else if login_info.Request=="relogin"{
			n,err:=redis.String(c.Do("HGET", "guid_id",login_info.Session_guid))
			if n==login_info.Id&&err==nil {
				log_reply_message.Content="AUTH"
				log_reply_message.Session_guid=login_info.Session_guid
				log_reply_message.Id=login_info.Id
				}
		//注册处理
		}else if login_info.Request=="regist" {
			
			is_usable, err := redis.Bool(c.Do("HSETNX", "user_auth",login_info.Id,login_info.Password))
			if err == nil&&is_usable==true{
				log_reply_message.Content="AUTH"
				//guid处理
				log_reply_message.Session_guid=UniqueId(login_info.Id)
				log_reply_message.Id=login_info.Id
				_,err=c.Do("HSET", "id_guid",login_info.Id,log_reply_message.Session_guid)
				_,err=c.Do("HSET", "guid_id",log_reply_message.Session_guid,login_info.Id)
					
			}else {
				log_reply_message.Content="UNEXCEPTECED ERROR"
			}
    		
		//default处理
		}else {
			
			log_reply_message.Content="UNRECOGNIZED REQUEST"	
		}
		//返回结果
		login_info.log_chan<-*log_reply_message


	}
}

//请求处理(检查guid是否依旧有id权限,将消息照表送入相应的id bucket,检查id bucket内的消息,并返回请求)
func request_process(request_process_chan chan request_info,id_guid_chan chan id_guid_pair){
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
    if err != nil {
        fmt.Println("Connect to redis error", err)
        return
    }
	defer c.Close()
	
	id_guid:=id_guid_pair{}
	for {
		reply_content:=new(reply_info)
		Request:=<-request_process_chan	
		//先校验guid
		Id,err:=redis.String(c.Do("HGET", "guid_id",Request.Session_guid))
		if Id==""||err!=nil{
			reply_content.Reply="OCCUPIED"
			Request.reply_chan<-*reply_content	
			continue			
		}
		//发送消息
		if Request.Request=="send_message"{	
			for _,one_message:=range Request.Messages{
				json_string,_:=json.Marshal(one_message)
				_,err:=c.Do("SADD", one_message.Receiver_id,json_string)
				if err!=nil{
					log.Println(err)
					continue
				}
				//检查是否在线,在线就准备推送
				guid,err:=redis.String(c.Do("HGET", "id_guid",one_message.Receiver_id))
				if guid!=""&&err!=nil{
					id_guid.Id=one_message.Receiver_id
					id_guid.Guid=guid
					id_guid_chan<-id_guid
				}
			}
			
		reply_content.Reply="OK"
		Request.reply_chan<-*reply_content	
		}
	//主动拉取消息
		if Request.Request=="get_message"{
			number,err:=redis.Int(c.Do("SCARD",Request.Id))
		    if err!=nil {
				reply_content.Reply="ERROR"
			}else {
				reply_content.Reply="OK"
				reply_content.Messages,err=redis.Strings(c.Do("SPOP",Request.Id,number))
			}
			Request.reply_chan<-*reply_content		
		}
	}

}
//发送失败的消息回收送入redis
func recollect_process(recollect_process_chan chan []string){
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
    if err != nil {
        fmt.Println("Connect to redis error", err)
        return
    }
	defer c.Close()
	count:=0
	t := time.NewTimer(time.Millisecond * 5)
	message_recycle:=message{}
	for {
		select{
		case Messages:=<-recollect_process_chan :
			for _,one_message:=range Messages{
			err:=json.Unmarshal([]byte(one_message),message_recycle)
			if err!=nil{
				 log.Println(err)
			}
			c.Send("SADD",message_recycle.Receiver_id,one_message)
			count++
			if count>100{
				c.Flush()
			}
			}
		case <-t.C:
			t.Reset(time.Millisecond * 5)
			c.Flush()	
			count=0		
		}
	}

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