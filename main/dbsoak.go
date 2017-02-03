package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MyDoc struct {
	myid      int
	timestamp time.Time
	value     int
}

var (
	wantMonotonic = false
	IsDrop        = false
	UseIndex      = false
	DoReads       = false
	batchSize     = 1000
	err           error
	totalDocs     = 5 * 1000 * 1000
	reportEach    = 100 * 1000
	doing         = "upserts"
)

func timestamp(doing string, done int, batchSize int, startTime int64) {
	elapsedTime := time.Duration(time.Now().UnixNano() - startTime)
	fmt.Printf("%s go %s %d / %d (%d per batch) in %s = %.1f/s\n", time.Now().String(), doing, done, totalDocs, batchSize, elapsedTime, float64(done)/(elapsedTime.Seconds()))
}

func main() {
	for i := range os.Args {
		if i < 1 {
			continue
		}
		arg := os.Args[i]
		var numArg int
		if arg == "upserts" || arg == "inserts" || arg == "replaces" {
			doing = arg
		} else if numArg, err = strconv.Atoi(arg); err != nil {
			log.Panicf("Bad number %s", arg)
		} else if numArg <= 1000 {
			batchSize = numArg
		} else {
			totalDocs = numArg
		}
	}

	session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	if wantMonotonic {
		session.SetMode(mgo.Monotonic, true)
	}

	// Drop Database
	if IsDrop {
		if err = session.DB("dbsoak").DropDatabase(); err != nil {
			panic(err)
		}
	}

	// Collection go
	c := session.DB("dbsoak").C("go")
	bulk := c.Bulk()
	bulk.Unordered()

	if UseIndex {
		// Index
		index := mgo.Index{
			Key:        []string{"name", "phone"},
			Unique:     true,
			DropDups:   true,
			Background: true,
			Sparse:     true,
		}

		if err = c.EnsureIndex(index); err != nil {
			panic(err)
		}
	}

	startTime := time.Now().UnixNano()
	batchNext, totals := 0, 0
	for i := 0; i < totalDocs; i++ {
		myId := i % 1000
		if batchSize == 0 {
			// Insert Datas
			if err = c.Insert(&MyDoc{myId, time.Now(), i}); err != nil {
				panic(err)
			}
			continue
		}
		if doing == "upserts" {
			bulk.Upsert(bson.M{"myid": myId}, bson.M{"$set": bson.M{"timestamp": time.Now(), "value": i}})
		} else if doing == "replaces" {
			panic("No replaces implemented") // bulk.Upsert(bson.M{"myid": myId}, bson.M{"$set": bson.M{"timestamp": time.Now(), "value": i}})
		} else if doing == "inserts" {
			bulk.Insert(&MyDoc{myId, time.Now(), i})
		}
		batchNext++
		if batchNext == batchSize {
			bulkRes, err := bulk.Run()
			if err != nil {
				panic(err)
			}
			totals += bulkRes.Matched + bulkRes.Modified
			batchNext = 0
			bulk = c.Bulk()
		}
		if i%reportEach == 0 {
			timestamp(doing, i, batchSize, startTime)
		}
	}
	if batchNext != 0 {
		bulkRes, err := bulk.Run()
		if err != nil {
			panic(err)
		}
		totals += bulkRes.Matched + bulkRes.Modified
	}

	if DoReads {
		// Query One
		result := MyDoc{}
		if err = c.Find(bson.M{"myId": 100}).Select(bson.M{"value": 1010}).One(&result); err != nil {
			panic(err)
		}
		fmt.Println("Phone", result)

		// Query All
		var results []MyDoc
		if err = c.Find(bson.M{"myId": "Ale"}).Sort("-timestamp").All(&results); err != nil {
			panic(err)
		}
		fmt.Println("Results All: ", results)

		// Update
		colQuerier := bson.M{"name": "Ale"}
		change := bson.M{"$set": bson.M{"phone": "+86 99 8888 7777", "timestamp": time.Now()}}
		if err = c.Update(colQuerier, change); err != nil {
			panic(err)
		}

		// Query All
		if err = c.Find(bson.M{"name": "Ale"}).Sort("-timestamp").All(&results); err != nil {
			panic(err)
		}
		fmt.Println("Results All: ", results)
	}

	//elapsedTime := time.Duration(time.Now().UnixNano() - startTime)
	//fmt.Printf("%s go inserts: %d / %d (%d per batch) in %s = %.1f/s\n", time.Now().String(), totals, totalDocs, batchSize, elapsedTime, float64(totalDocs)/(elapsedTime.Seconds()))
	timestamp(doing, totalDocs, batchSize, startTime)
}
