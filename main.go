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
var db, err = sql.Open("mysql", "mcolumn:mpass@tcp("+host+")/flux_saas")
var fpath = "C:/Users/User 2/projects/bigquery-streaming/some5.csv"
var table = "flat5"

// TimeQuery
func TimeQuery(sqlString string) string {
	t0 := time.Now()
	rows, err := db.Query(sqlString)
	var columns []string
	columns, err = rows.Columns()
	columnLength := len(columns)

	ctr := 0

	for rows.Next() {
		ctr = ctr + 1		
		var	items interface{}
		
		cols := make([]interface{}, columnLength)
		for i := 0; i < columnLength; i++ {
			cols[i] = &items
		}
		err = rows.Scan(cols...)
	}
	
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
		// for x := 0; x < 5; x++ {
		// _ ,err := db.Query(`reset query cache;`)
		// if (err != nil){
		// 	fmt.Println(err)
		// }
		// _ ,err1 := db.Query(`flush query cache;`)
		// if (err1 != nil){
		// 	fmt.Println(err1)
		// }

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
	// }
		
	}

}
