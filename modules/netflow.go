package modules

import (
	"database/sql"
	"log"

	"fmt"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	//"sync"
	"time"
)

var DbHdl *sql.DB

func Db(usr, pwd, host, db string) {
	var err error

	connParams := usr + ":" + pwd + "@tcp(" + host + ":3306)/" + db
	DbHdl, err = sql.Open("mysql", connParams)
	if err != nil {
		log.Fatal(err)
	}

	err = DbHdl.Ping()
	if err != nil {
		log.Fatal(err)
	}
}

func TblMaxMinTime(ctg, srcTbl string) int {
	var time int

	sqlStr := "select " + ctg + "(time) from " + srcTbl
	err := DbHdl.QueryRow(sqlStr).Scan(&time)

	if err != nil {
		log.Fatal(err)
	}

	return time
}

func EtcdTime(key string) int {
	var time int
	var err error

	bytes := EtcdGet(key)

	if len(bytes) == 0 {
		time = -1
	} else {
		time, err = strconv.Atoi(string(bytes))
		if err != nil {
			log.Fatal(err)
		}
	}

	return time
}

func EtcdRecord(key, val string) {
	EtcdSet(key, val)
}

func BeginTime(etcdTime, tblMinTime, interval int) int {
	/*var time int

	  if  etcdTime < tblMinTime {
	      time = (tblMinTime / interval) * interval
	      fmt.Println("tbl-time:", time)
	  } else {
	      time = (etcdTime / interval) * interval
	      fmt.Println("etcd-time:", time)
	  }

	  return time
	*/

	return etcdTime
}

func FiltData(srcTbl string, dstTbl string, beginTime int, endTime int, groupBys []string) bool {
	var groupByStr string
	var sqlStr string
	groupBysLen := len(groupBys)

	startSelect := time.Now().Unix()

	if groupBysLen > 0 {
		for key, val := range groupBys {
			if key < groupBysLen-1 {
				groupByStr = groupByStr + val + ", "
			} else {
				groupByStr = groupByStr + val
			}
		}
		sqlStr = "select " + groupByStr + ", sum(flow) from " + srcTbl +
			" where time >= " + strconv.Itoa(beginTime) + " and time <= " + strconv.Itoa(endTime) +
			" group by " + groupByStr
	} else {
		sqlStr = "select sum(flow) from " + srcTbl +
			" where time >= " + strconv.Itoa(beginTime) + " and time <= " + strconv.Itoa(endTime)
	}

	rows, err := DbHdl.Query(sqlStr)

	endSelect := time.Now().Unix()

	fmt.Println("select use time: ", endSelect-startSelect)

	defer rows.Close()

	if err != nil {
		log.Fatal(err)
	}

	startInsert := time.Now().Unix()

	for rows.Next() {
		var err error
		var sqlStr string
		var sum int
		var groupBy0 string
		var groupBy1 string

		if groupBysLen == 0 {
			err = rows.Scan(&sum)
			if err != nil {
				return false
			}
			sqlStr = "insert into " + dstTbl + " (time, flow) values (" +
				strconv.Itoa(endTime) + ", " + strconv.Itoa(sum) + ")"
		} else if groupBysLen == 1 {
			err = rows.Scan(&groupBy0, &sum)
			if err != nil {
				return false
			}
			sqlStr = "insert into " + dstTbl + " (time, flow, " + groupBys[0] + ") values (" +
				strconv.Itoa(endTime) + ", " + strconv.Itoa(sum) + ", '" + groupBy0 + "')"
		} else {
			err = rows.Scan(&groupBy0, &groupBy1, &sum)
			if err != nil {
				return false
			}
			sqlStr = "insert into " + dstTbl + " (time, flow, " + groupBys[0] + ", " + groupBys[1] + ") values (" +
				strconv.Itoa(endTime) + ", " + strconv.Itoa(sum) + ", '" + groupBy0 + "', '" + groupBy1 + "')"
		}
		if err != nil {
			log.Fatal(err)
		}

		stmt, err := DbHdl.Prepare(sqlStr)
		if err != nil {
			log.Fatal(err)
		}

		endInsert := time.Now().Unix()

		fmt.Println("insert use time: ", endInsert-startInsert)
		defer stmt.Close()

		_, err = stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}
	}

	return false
}
