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
	"time"
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