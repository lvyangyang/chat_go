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
//	"crypto/md5"  
 //   "crypto/rand"  
//	"encoding/base64" 
//	"encoding/hex"  
//	"io"
	
//	"reflect"
//	"bytes"
)
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
					reply_content.Reply="ERROR"
				}else { 
					reply_content.Reply="OK"
					}

				push_channel.Messages_chan<-reply_content	
				}						
		}
	}

}

