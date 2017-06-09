package main

import (
    //"fmt"
    "./modules"
)

func main () {
    var tblPostfix string = "_quarter" 
    var timeWindow int = 15

    modules.InitEtcdCli()
    modules.Db("root", "mysqladmin", "10.88.1.102", "aptwebservice")
    go modules.Sum("tbl_netflowd", "tbl_netflowd" + tblPostfix, timeWindow, []string{"direction"}, "apt/sum/")
    go modules.Sum("tbl_netflowp", "tbl_netflowp" + tblPostfix, timeWindow, []string{"protocol"}, "apt/sum/")
    go modules.Sum("tbl_netflowip", "tbl_netflowip" + tblPostfix, timeWindow, []string{"assetIP"}, "apt/sum/")
    go modules.Sum("tbl_netflowipd", "tbl_netflowipd" + tblPostfix, timeWindow, []string{"assetIP","direction"}, "apt/sum/")
    go modules.Sum("tbl_netflowipp", "tbl_netflowipp" + tblPostfix, timeWindow, []string{"assetIP","protocol"}, "apt/sum/")
    go modules.Sum("tbl_netflowdp", "tbl_netflowdp" + tblPostfix, timeWindow, []string{"direction","protocol"}, "apt/sum/")

    ch := make(chan int, 4)
    <- ch
}
