package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"go.mongodb.org/mongo-driver/mongo/options"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

const (
	uri                  = ""
	serverConnectTimeout = 20 * time.Second
	pingTimeout          = 20 * time.Second
	batchSize            = 100
)

type Progress struct {
	ChangeStreamEvents atomic.Uint64
	numInserts         uint64
}

func main() {
	clientOptions := moptions.Client().ApplyURI(uri)
	ctxConnect, cancel := context.WithTimeout(context.Background(), serverConnectTimeout)
	defer cancel()
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	ctxPing, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	err = client.Ping(ctxPing, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to CosmosDB!")
	//try with 13 collections
	var locations = []Location{
		{Collection: "col1", Database: "db1"},
		{Collection: "col2", Database: "db1"},
		{Collection: "col1", Database: "db2"},
		{Collection: "col2", Database: "db2"},
		{Collection: "col1", Database: "db3"},
		{Collection: "col2", Database: "db3"},
		{Collection: "col1", Database: "db4"},
		{Collection: "col2", Database: "db4"},
		/*
			{Collection: "col1", Database: "db5"},
			{Collection: "col2", Database: "db5"},
			{Collection: "col1", Database: "db6"},
			{Collection: "col2", Database: "db6"},
			{Collection: "col1", Database: "db7"}, */
	}

	// Progress struct to compare the speed of inserts to change stream events

	progress := Progress{
		numInserts: 0,
	}
	progress.ChangeStreamEvents.Store(0)

	//run concurrent change streams and inserts generator, print the progress every 5 seconds
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		concurrentChangeStreams(locations, client, &progress)
	}()

	go func() {
		defer wg.Done()
		for {
			insertsGenerator(locations, client, &progress)
		}
	}()

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		startTime := time.Now()
		inserts := progress.numInserts
		changeEvents := progress.ChangeStreamEvents.Load()
		for {
			select {
			case <-ticker.C:
				//inserts per second
				elapsed := time.Since(startTime).Seconds()
				insertsDelta := progress.numInserts - inserts
				insertsPerSecond := math.Floor(float64(insertsDelta) / elapsed)
				inserts = progress.numInserts
				//change stream events per second
				changeEventsDelta := progress.ChangeStreamEvents.Load() - changeEvents
				changeEventsPerSecond := math.Floor(float64(changeEventsDelta) / elapsed)
				changeEvents = progress.ChangeStreamEvents.Load()
				fmt.Printf("Progress Check: Change Stream Events - %d, Inserts - %d, Inserts Per Second - %f, Change Events Per Second - %f \n", progress.ChangeStreamEvents.Load(), progress.numInserts, insertsPerSecond, changeEventsPerSecond)
				startTime = time.Now()
			}
		}
	}()

	wg.Wait()
	disconnect(client)

}

func disconnect(client *mongo.Client) {
	ctxDisconnect, cancel := context.WithTimeout(context.Background(), serverConnectTimeout)
	defer cancel()
	err := client.Disconnect(ctxDisconnect)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Disconnected from CosmosDB!")
}

type Location struct {
	Collection string
	Database   string
}

func createChangeStream(namespace Location, client *mongo.Client, ctx context.Context) *mongo.ChangeStream {
	db := namespace.Database
	col := namespace.Collection
	collection := client.Database(db).Collection(col)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "replace"}}}}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}, {Key: "fullDocument", Value: 1}, {Key: "ns", Value: 1}, {Key: "documentKey", Value: 1}}}},
	}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	changeStream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		fmt.Printf("Error opening change stream: %v\n", err)
		return nil
	}
	fmt.Printf("Opened change stream for %v\n", collection)
	return changeStream
}

func concurrentChangeStreams(namespaces []Location, client *mongo.Client, progress *Progress) {
	var wg sync.WaitGroup
	for _, namespace := range namespaces {
		wg.Add(1)
		go func(namespace Location) {
			defer wg.Done()
			ctxChangeStream, cancel := context.WithCancel(context.Background())
			defer cancel()
			changeStream := createChangeStream(namespace, client, ctxChangeStream)
			defer changeStream.Close(ctxChangeStream)

			for changeStream.Next(ctxChangeStream) {
				var event bson.M
				if err := changeStream.Decode(&event); err != nil {
					fmt.Printf("Could not decode change event: %v\n", err)
					continue
				}
				progress.ChangeStreamEvents.Add(1)
			}
		}(namespace)
	}

	wg.Wait()
}

func setUpNamespaces(client *mongo.Client, locations []Location) {
	for _, namespace := range locations {
		db := namespace.Database
		col := namespace.Collection
		collection := client.Database(db).Collection(col)
		ctxInsert, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := collection.InsertOne(ctxInsert, bson.D{{Key: "randomNumber", Value: rand.Int()}})
		if err != nil {
			fmt.Printf("Error inserting document into %v: %v\n", collection, err)
		}
	}
	fmt.Printf("Set up each namespace \n")
}

// Inserts document into each collection
func insertsGenerator(namespaces []Location, client *mongo.Client, progress *Progress) {
	for _, namespace := range namespaces {
		db := namespace.Database
		col := namespace.Collection
		collection := client.Database(db).Collection(col)
		ctxInsert, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		batch := make([]interface{}, batchSize)
		for i := 0; i < batchSize; i++ {
			document := bson.D{{Key: "randomNumber", Value: rand.Int()}}
			batch[i] = document
		}
		_, err := collection.InsertMany(ctxInsert, batch)
		if err != nil {
			fmt.Printf("Error inserting document into %v: %v\n", collection, err)
		}
		progress.numInserts += batchSize
	}

}
