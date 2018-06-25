package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"encoding/binary"
	"bytes"
	"bufio"
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
	Time_stamp string `json:"Time_stamp"`
	Request string `json:"Request"`
	Session_guid string `json:"Session_guid"`
	log_chan chan auth_info 
}

type reply_info struct{
	Reply string `json:"Reply"`
	Messages []string `json:"Messages"`
}

type request_info struct{
	Request string `json:"Request"`
	Session_guid string `json:"Session_guid"`
	Messages []message `json:"Messages"`
	reply_chan chan reply_info
}

func main() {
	tcpAddr:="127.0.0.1:2563"
	conn, err := net.Dial("tcp", tcpAddr)
	checkError(err)
	user_info:=make(map[string]string)
	user_info["id"]="1234541"
	user_info["password"]="0215151"
	user_info["request"]="login"
	json_string,err:=json.Marshal(user_info)

	send_string:=write_content(json_string)
	string_length, err := conn.Write([]byte(send_string))
	fmt.Println(string_length)
	checkError(err)
	result, err := ioutil.ReadAll(conn)
	checkError(err)
	fmt.Println(string(result))
	os.Exit(0)
}
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
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