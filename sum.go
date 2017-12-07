package main

import (
	"fmt"
	"os"
	"strconv"
	"sum/modules"
	"sync"
)

var wg sync.WaitGroup

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
	modules.Db("root", "mysqladmin", "10.88.1.102", "aptwebservice")

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
