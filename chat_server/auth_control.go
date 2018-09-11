package main

import(
	"fmt"
//	"net"
//	"log"
//	"io"
//	"bufio"
//	"time"
//	"strconv"
//	"encoding/json"
//	"encoding/binary"
//	"bytes"
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