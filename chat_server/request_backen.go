package main

import(
	"fmt"
//	"net"
	"log"
//	"io"
//	"bufio"
//	"time"
//	"strconv"
	"encoding/json"
//	"encoding/binary"
//	"bytes"
//	"time"
//	"reflect"
	"github.com/garyburd/redigo/redis"
//	"crypto/md5"  
//    "crypto/rand"  
//	"encoding/base64" 
//	"encoding/hex"  
//	"io"
	
//	"reflect"
//	"bytes"
)

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
				if guid!=""||err!=nil{
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