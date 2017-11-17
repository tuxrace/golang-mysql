package main

import (
	"context"
	"fmt"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/bigquery"
)

func main() {
	ctx := context.Background()
	projectID := "project-flux-testing"
	client, err := bigquery.NewClient(ctx, projectID)
	client.Dataset("flux").Table("traffic_data")
	q := client.Query(`INSERT INTO [project-flux-testing:flux.with_json2] trafficId values ('1')`)
	// q.Dst = client.Dataset("flux").Table("traffic_data")
	q.UseLegacySQL = true

	it, err := q.Read(ctx)

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		fmt.Println(values)
	}
	if err != nil {
		fmt.Println(err, it)
	}

}
