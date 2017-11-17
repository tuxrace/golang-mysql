package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
)

var host = os.Getenv("HOST")
var db, err = sql.Open("mysql", "root:clusterix@tcp("+host+")/flux_saas")
var fpath = "C:/Users/User 2/projects/bigquery-streaming/some5.csv"
var table = "flat4"

// TimeQuery
func TimeQuery(sqlString string) string {
	t0 := time.Now()
	rows, err := db.Query(sqlString)
	var columns []string
	columns, err = rows.Columns()
	colNum := len(columns)

	ctr := 0

	for rows.Next() {
		ctr = ctr + 1
		var (
			id string
		)
		cols := make([]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			cols[i] = &id
		}
		err = rows.Scan(cols...)
	}
	// time.Sleep(500 * time.Millisecond)
	t1 := time.Now()
	elapsed := t1.Sub(t0)
	if err != nil {
		fmt.Println(err, rows)
		os.Exit(3)
	}
	rows.Close()
	return elapsed.String() + " Rows: " + strconv.Itoa(ctr)
}

func main() {
	mysql.RegisterLocalFile(fpath)

	if err != nil {
		panic(err.Error())
	} else {

		r1, err := db.Query(`flush query cache;`)
		fmt.Println("Cleared", r1, err)

		res1 := TimeQuery("SELECT count(*), DATE(trafficTimestamp) as dt FROM flux_saas." + table + " where trafficTimestamp >= '2017-10-01 00:00:00' and trafficTimestamp < '2017-10-31 23:59:59' group by dt")
		fmt.Println("Query 1: ", res1)

		res2 := TimeQuery("SELECT count(*), funnelId, trafficSourceId, locationCountry FROM flux_saas." + table + " where trafficTimestamp >= '2017-10-01 00:00:00' and trafficTimestamp < '2017-10-31 23:59:59'	group by funnelId, trafficSourceId, locationCountry")
		fmt.Println("Query 2: ", res2)

		res3 := TimeQuery("SELECT count(*), funnelId, trafficSourceId, nodeTypeId FROM flux_saas." + table + " where trafficTimestamp >= DATE_SUB('2017-10-31 23:59:59', INTERVAL 1 WEEK) and trafficTimestamp < curdate() group by  funnelId, trafficSourceId, nodeTypeId")
		fmt.Println("Query 3: ", res3)

		res4 := TimeQuery("SELECT count(*), hitId FROM flux_saas." + table + "	where trafficTimestamp >= DATE_SUB('2017-10-31 23:59:59', INTERVAL 1 WEEK)	group by hitId having count(*) >= 1	order by count(*) desc limit 1")
		fmt.Println("Query 4: ", res4)

		res5 := TimeQuery("SELECT count(*), trafficSourceId, funnelId, DAYOFWEEK(trafficTimestamp) as dow FROM flux_saas." + table + " where trafficTimestamp >= '2017-10-01 00:00:00' and trafficTimestamp < '2017-10-31 23:59:59'	group by trafficSourceId, funnelId, dow")
		fmt.Println("Query 5: ", res5)

	}

}
