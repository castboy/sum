package main

import (
	"fmt"
	"os"
	"strconv"
	"sum/modules"
	"sync"

	"github.com/widuu/goini"
)

var wg sync.WaitGroup

type Conf struct {
	Host   string
	User   string
	Passwd string
	Db     string
}

func Sum(srcTbl string, dstTbl string, interval int, groupBys []string, etcdDir string) {
	tblMaxTime := modules.TblMaxMinTime("max", srcTbl)
	tblMinTime := modules.TblMaxMinTime("min", srcTbl)
	etcdTime := modules.EtcdTime(etcdDir + dstTbl)
	beginTime := modules.BeginTime(etcdTime, tblMinTime, interval*60)
	endTime := beginTime + interval*60

	if endTime+interval*60 < tblMaxTime {
		modules.EtcdRecord(etcdDir+dstTbl, strconv.Itoa(endTime))
		fmt.Println("endTime:", endTime)
		modules.FiltData(srcTbl, dstTbl, beginTime, endTime, groupBys)
	}

	fmt.Println(dstTbl, " executed")
	wg.Add(-1)
}

func getConf() Conf {
	conf := goini.SetConfig("conf.ini")

	return Conf{
		Host:   conf.GetValue("mysql", "host"),
		User:   conf.GetValue("mysql", "user"),
		Passwd: conf.GetValue("mysql", "passwd"),
		Db:     conf.GetValue("mysql", "db"),
	}
}

func main() {
	var tblPostfix string = "_h"
	var timeWindow int = 60

	args := os.Args
	if args != nil && len(args) == 3 {
		var err error
		timeWindow, err = strconv.Atoi(args[1])
		if err != nil {
			fmt.Println(err)
		}
		tblPostfix = args[2]
	}

	fmt.Println("timeWindow:", timeWindow)

	modules.InitEtcdCli()
	conf := getConf()
	modules.Db(conf.User, conf.Passwd, conf.Host, conf.Db)

	var tbls map[string][]string
	tbls = make(map[string][]string)
	tbls["netflow"] = []string{}
	tbls["netflowd"] = []string{"direction"}
	tbls["netflowp"] = []string{"protocol"}
	tbls["netflowip"] = []string{"assetIp"}
	tbls["netflowipd"] = []string{"assetIp", "direction"}
	tbls["netflowipp"] = []string{"assetIp", "protocol"}
	tbls["netflowdp"] = []string{"direction", "protocol"}

	for tbl, groupBys := range tbls {
		wg.Add(1)
		go Sum(tbl, tbl+tblPostfix, timeWindow, groupBys, "apt/sum/")
	}

	wg.Wait()
}
