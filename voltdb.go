/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

// A simple example that demonstrates the use of the VoltDB database/sql/driver.
package main

import (
	"time"
	"database/sql"
	"fmt"
	"log"

	"github.com/VoltDB/voltdb-client-go/voltdbclient"
)

func main() {
	// If using a version of VoltDB server prior to 5.2, then
	// set the version of the wire protocol to 0.  The default
	// value 1, indicates a server version of 5.2 or later.
	voltdbclient.ProtocolVersion = 1

	db, err := sql.Open("voltdb", "//35.195.245.85:21212")
	if err != nil {
		log.Fatal(err)
	}
	// defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := db.Prepare(`SELECT count(*), trafficSourceId, funnelId, DAYOFWEEK(trafficTimestamp) as dow FROM flat4 where trafficTimestamp >= '2017-10-01 00:00:00' and trafficTimestamp < '2017-10-31 23:59:59' group by trafficSourceId, funnelId, dow`)
	if err != nil {
		log.Fatal(err)
	}

	t0 := time.Now()
	rows, err := stmt.Query()
	if err != nil {
		log.Fatal(err)
	}

	printRows(rows)
	t1 := time.Now()

	fmt.Println("----",t1.Sub(t0))
}

func printRows(rows *sql.Rows) {
	for rows.Next() {
		var item interface{}		
		err := rows.Scan(&item)
		if err != nil {
			fmt.Println(err)
			break
		}
		// fmt.Printf("%v", item)
	}
}