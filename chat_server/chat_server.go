package main

import(
//	"fmt"
	"net"
	"log"
//	"io"
//	"bufio"
//	"time"
//	"strconv"

//"encoding/json"
//	"encoding/binary"
//	"bytes"
//	"time"
//	"reflect"
//	"github.com/garyburd/redigo/redis"
//	"crypto/md5"  
//    "crypto/rand"  
//	"encoding/base64" 
//	"encoding/hex"  
//	"io"
	
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
	var chan_buffer_size=1
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
	i:=0
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
		for {
			conn_push,err:=listener_push.Accept()
			if err!=nil{
				log.Fatal(err)
			}
			go handle_push(conn_push,login_process_chan,&push_conn_table,recollect_process_chan )
		}
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
		go handle_conn(conn,request_process_chan,login_process_chan,recollect_process_chan,kick_off_list_chan)
	}
	
}



