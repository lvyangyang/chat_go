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
	"reflect"
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

type login_info struct{
	id string
	password string
	time_stamp string
	request string
	log_chan chan message
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
	//请求处理过程
	request_process_chan:=make(chan message,chan_buffer_size)
	//身份验证过程
	login_process_chan:=make(chan login_info,chan_buffer_size)
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

func handle_conn(conn net.Conn,user_table * User_Table,request_process_chan chan message,login_process_chan chan login_info){
	
	content_buff,err:=read_content(conn)
	if err!=nil{
		conn.Close()
		return 
	}
	//解析json内容
	var user_info =make(map[string]string)
	conn.Write([]byte("ok"))
	err=json.Unmarshal(content_buff, &user_info)
	if err!=nil{
		conn.Close()
		return
	}
	fmt.Println(user_info)
	id:=user_info["id"]
	//检查权限
	recv_log := login_info{}
    err = json.Unmarshal(content_buff, &recv_log)
    if err != nil {

        log.Println(err)
        return
	}
	recv_log.log_chan=make(chan message)
	login_process_chan<-recv_log

	auth_message:=<-recv_log.log_chan
	if auth_message.message_content!="AUTH"{
		return
	}

	//暂无
	var this_session session
	this_session.tcp_connection=conn
	this_session.conn_time_stamp=time.Now().Unix()
	this_session.send_channel=recv_log.log_chan
	(*user_table)[id]=this_session


for {
	content_buff,err=read_content(conn)
	if err!=nil{
		conn.Close()
		return 
	}
	recv_m := message{}

    err := json.Unmarshal(content_buff, &recv_m)
    if err != nil {

        log.Println(err)
        return
	}
	recv_m.send_time_stamp=time.Now().Unix()
	recv_m.sender_id=user_info["id"]
	recv_m.sender_ip=conn.RemoteAddr().String()
	//请求送入处理过程
	request_process_chan<-recv_m
	//获取处理结果,发送数据
	send_m:=<-this_session.send_channel
	t := reflect.TypeOf(send_m)
	v := reflect.ValueOf(send_m)

	var data = make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
	data[t.Field(i).Name] = v.Field(i).Interface()
	}
		json_string,_:=json.Marshal(data)
		send_string:=write_content(json_string)
		_, err = conn.Write([]byte(send_string))
}

}

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
	
}