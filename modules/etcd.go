//etcd.go

package modules

import (
    "os"
    "fmt"
    "log"
    "time"
    "errors"
    "golang.org/x/net/context"
    "github.com/coreos/etcd/clientv3"
)

var EtcdCli *clientv3.Client

func InitEtcdCli() {
    cfg := clientv3.Config{
        Endpoints:               []string{"http://10.88.1.103:2379"},
        DialTimeout: 10 * time.Second,
    }
    var err error = errors.New("this is a new error")
    EtcdCli, err = clientv3.New(cfg)
    if err != nil {
        log.Fatal(err)
        //errLog := "InitEtcdCli Err"
        //Log("Err", errLog)
    }
    //defer EtcdCli.Close()
}

func EtcdSet(k, v string) {
    ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
    resp, err := EtcdCli.Put(ctx, k, v)
    cancel()
    if err != nil {
        fmt.Println("EtcdSetErr")
        //errLog := "EtcdSetErr"
        //Log("Err", errLog)
    } else {
        //fmt.Println(string(resp.Kvs[0].Value))    
        fmt.Println("set ", k, "success. times:", resp.Header.Revision)    
        //infoLog := "set " + k + "success. times:"+ string(resp.Header.Revision)
        //Log("Info", infoLog)
    }
}

func EtcdGet(key string) []byte {
    defer func()  {
        if r := recover(); r != nil {
            errInfo := "configuration item: " + key + " does not exist!"
            fmt.Println(errInfo)    
            //Log("Err", errInfo)
            os.Exit(0)
        }    
    }()
    
    ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
    resp, err := EtcdCli.Get(ctx, key)
    cancel()
    if err != nil {
        log.Fatal(err)
        //panic("")
    }
    bytes := resp.Kvs[0].Value   
    
    return bytes
}
