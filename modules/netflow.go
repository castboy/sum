package modules

import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "log"
    //"fmt"
    "strconv"
    //"sync"
)

var DbHdl *sql.DB

func Db (usr, pwd, host, db string) {
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

func TblMaxMinTime (ctg, srcTbl string) int {
    var time int

    sqlStr := "select " + ctg + "(time) from " + srcTbl
    err := DbHdl.QueryRow(sqlStr).Scan(&time)

    if err != nil {
        log.Fatal(err)
    }

    return time 
}

func EtcdTime (key string) int {
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

func EtcdRecord (key, val string) {
    EtcdSet(key, val)
}

func BeginTime (etcdTime, tblMinTime, interval int) int {
    var time int

    if  etcdTime < tblMinTime {
        time = (tblMinTime / interval) * interval 
    } else {
        time = (etcdTime / interval) * interval
    }

    return time
}

func FiltData (srcTbl string, dstTbl string, beginTime int, endTime int, groupBys []string) {
    var groupByStr string
    groupBysLen := len(groupBys)
    for key, val := range groupBys {
        if key < groupBysLen - 1 {
            groupByStr = groupByStr + val + ", "
        } else {
            groupByStr = groupByStr + val
        }
    }

    sqlStr := "select " + groupByStr + ", sum(flow) from " +  srcTbl +  
              " where time >= " + strconv.Itoa(beginTime) + " and time <= " + strconv.Itoa(endTime) + 
              " group by " + groupByStr

    rows, err := DbHdl.Query(sqlStr)

    defer rows.Close()

    if err != nil {
        log.Fatal(err)
    }

    for rows.Next() {
        var err error
        var sqlStr string
        var sum int
        var groupBy0 string
        var groupBy1 string

        if len(groupBys) == 1 {
            err = rows.Scan(&groupBy0, &sum)
            sqlStr = "insert into " + dstTbl + " (time, flow, " + groupBys[0] +  ") values (" +
                      strconv.Itoa(endTime) + ", " + strconv.Itoa(sum) + ", '" + groupBy0 + "')" 
        } else {
            err = rows.Scan(&groupBy0, &groupBy1, &sum)
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
        
        defer stmt.Close()

        _, err = stmt.Exec()
        if err != nil {
            log.Fatal(err)
        }
    }
}

