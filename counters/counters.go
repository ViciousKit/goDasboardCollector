package counters

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var hubs = map[int]string{
	105292: "yappy",
}

var collection = map[int]string{
	105292: "socialNetworkCountersYappy",
}

type Config struct {
	CountersType string      `json:"countersType"`
	HubsConfig   []HubConfig `json:"hubsConfig"`
}

type HubConfig struct {
	HubId    int       `json:"hubId"`
	HubName  string    `json:"hubName"`
	Sections []Section `json:"sections"`
}

type Section struct {
	SectionName string `json:"sectionName"`
	PeriodName  string `json:"periodName"`
}

var periodNames = map[string][]string{
	"ba": {
		"new",
		"hour",
		"three_hours",
		"six_hours",
		"twelve_hours",
		"twelve_hours",
		"first_day",
		"day",
	},
	"stream": {
		"day",
		"week",
		"month",
	},
	"retro": {
		"new",
	},
}

func SocialCountersWorker() {
	mongoHost := "mongodb://localhost:27017"

	fmt.Println("counters worker")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoHost))
	if err != nil {
		log.Fatalln(err)
	}
	defer client.Disconnect(ctx)
	pingErr := client.Ping(ctx, nil)
	if pingErr != nil {
		log.Println(pingErr)

		return
	}
	fmt.Println("Ping successfull")

	config := prepareConfig()
	for _, cfg := range config {
		for _, hubConfig := range cfg.HubsConfig {
			processHub(hubConfig, cfg.CountersType, ctx, client)
		}
	}
}

func processHub(hubConfig HubConfig, cType string, ctx context.Context, client *mongo.Client) {
	var wg sync.WaitGroup
	for _, hubSection := range hubConfig.Sections {
		query := getQueryForSection(hubConfig.HubId, hubSection.PeriodName, hubSection.SectionName, cType)
		if query == nil {
			return
		}

		collection := client.Database("apiServices").Collection(collection[hubConfig.HubId])
		wg.Add(1)
		go func(query primitive.M, sectionName string) {
			defer wg.Done()
			result, _ := collection.CountDocuments(ctx, query)
			fmt.Printf("Stats for %s: %d\n", sectionName, result)
		}(query, hubSection.SectionName)
	}
	wg.Wait()
}

func getQueryForSection(HubId int, periodName string, sectionName string, cType string) primitive.M {
	status, origin, err := getStatusAndOriginForType(cType)
	if err != nil {
		return nil
	}
	time := int(time.Now().Unix())

	return bson.M{
		"hub_id":      int(HubId),
		"section":     sectionName,
		"type":        "post",
		"status":      status,
		"start_at":    bson.M{"$lt": time},
		"in_progress": bool(false),
		"origin":      origin,
	}
}

func getStatusAndOriginForType(cType string) (status string, origin string, err error) {
	switch cType {
	case "ba":
		return "new", "ba", nil
	case "stream":
		return "stream", "stream", nil
	case "retro":
		return "new", "ba", nil
	default:
		return "", "", errors.New("Wrong counters type: " + cType)
	}
}

func prepareConfig() []Config {
	var countersConfig []Config

	for countersType, periods := range periodNames {
		var hubsConfig []HubConfig

		for hubId, hubName := range hubs {
			var hubSections []Section

			for _, periodsName := range periods {
				var section Section
				section.PeriodName = periodsName
				name, err := getSectionNameForHubCountersType(hubId, countersType, periodsName)
				if err != nil {
					continue
				}
				section.SectionName = name
				hubSections = append(hubSections, section)
			}
			hubConfig := &HubConfig{
				HubId:    hubId,
				HubName:  hubName,
				Sections: hubSections,
			}
			hubsConfig = append(hubsConfig, *hubConfig)
		}
		typeConfig := &Config{
			CountersType: countersType,
			HubsConfig:   hubsConfig,
		}
		countersConfig = append(countersConfig, *typeConfig)
	}
	// json, _ := json.Marshal(countersConfig)
	// fmt.Println(string(json))

	return countersConfig
}

func getSectionNameForHubCountersType(hubId int, countersType string, periodName string) (string, error) {
	switch countersType {
	case "retro":
		return fmt.Sprintf("counters_%s_%s_%s", hubs[hubId], periodName, "retro"), nil
	case "stream":
		return fmt.Sprintf("counters_stream_%s_%s", hubs[hubId], periodName), nil
	case "ba":
		return fmt.Sprintf("counters_%s_%s", hubs[hubId], periodName), nil
	default:
		return "", errors.New("Wrong counters type: " + countersType)
	}
}
