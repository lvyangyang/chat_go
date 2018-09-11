package main
import(
//	"fmt"
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
//	"github.com/garyburd/redigo/redis"
//	"crypto/md5"  
 //   "crypto/rand"  
//	"encoding/base64" 
//	"encoding/hex"  
//	"io"
	
//	"reflect"
//	"bytes"
)

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

func handle_conn(conn net.Conn,request_process_chan chan request_info,login_process_chan chan login_info,
	recollect_process_chan chan []string,kick_off_list_chan chan string){
	
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
		}
			
				
	}
	kick_off_list_chan<-auth_message.Session_guid

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