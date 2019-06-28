package main

import (
	"fmt"
	"github.com/solipsis/coincapV2/pkg/coincap"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

type AssetData struct {
	ObjectId bson.ObjectId `json:"id" bson:"_id"`
	Id string `bson:"id"`
	Rank string `bson:"rank"`
	Symbol string `bson:"symbol"`
	Name string `bson:"name"`
	Supply string `bson:"supply"`
	MaxSupply string `bson:"maxSupply"`
	MarketCapUsd string `bson:"marketCapUsd"`
	VolumeUsd24Hr string `bson:"volumeUsd24Hr"`
	PriceUsd string `bson:"priceUsd"`
	ChangePercent24Hr string `bson:"changePercent24Hr"`
	Vwap24Hr string `bson:"vWap24hr"`
	CreatedAt time.Time `bson:"createdAt"`
}

type AssetHistoryData struct {
	Id string `bson:"id"`
	AssetId bson.ObjectId `json:"id" bson:"assetId"`
	PriceUsd string `bson:"priceUsd"`
	Time coincap.Timestamp `bson:"timestamp"`
	Date int64 `bson:"date"`
	CreatedAt time.Time `bson:"createdAt"`
}


func main() {

	//addAssets()



	var allAssets, _ = getAssets()


	i := 0
	for val := range allAssets  {

		i++
		fmt.Println(i, "başla")
		fmt.Println(allAssets[val].Name, allAssets[val].PriceUsd)


		var endDateChanger = 0
		var startDateChanger = 2
		// setup the time range
		end := time.Now().AddDate(-endDateChanger, 0,0)
		start := time.Now().AddDate(-startDateChanger, 0,0)

		fmt.Println(start)
		fmt.Println(end)


		var history, err = getAssetHistory(allAssets[val].Id, start, end)


		if err != nil {
			panic(err)
		}



		var _, _ = addAssetHistory(allAssets[val].ObjectId, allAssets[val].Id, history)


		fmt.Println(i, " - 5 saniye ara")
		time.Sleep(time.Second * 5)






	}















}

func addAssets () (string, error) {

	session, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c := session.DB("cryptofon").C("assets")


	client := coincap.NewClient(nil)

	params := &coincap.AssetsRequest{
		Limit: 2000,
	}

	assets, timestamp, err := client.Assets(params)

	fmt.Println(assets, timestamp, err)


	for value := range assets {


		result := AssetData{}
		err = c.Find(bson.M{"id": assets[value].ID}).One(&result)
		if err != nil {

			err = c.Insert(&AssetData{
				bson.NewObjectId(),
				assets[value].ID,
				assets[value].Rank,
				assets[value].Symbol,
				assets[value].Name,
				assets[value].Supply,
				assets[value].MaxSupply,
				assets[value].MarketCapUsd,
				assets[value].VolumeUsd24Hr,
				assets[value].PriceUsd,
				assets[value].ChangePercent24Hr,
				assets[value].Vwap24Hr,
				time.Now(),
			})

		}else{


			err = c.Update(bson.M{
				"_id": result.ObjectId,
			}, bson.M{
				"$set": bson.M{
					"id": assets[value].ID,
					"rank": assets[value].Rank,
					"symbol": assets[value].Symbol,
					"name": assets[value].Name,
					"supply": assets[value].Supply,
					"maxSupply": assets[value].MaxSupply,
					"marketCapUsd": assets[value].MarketCapUsd,
					"volumeUsd24Hr": assets[value].VolumeUsd24Hr,
					"priceUsd": assets[value].PriceUsd,
					"vhangePercent24Hr": assets[value].ChangePercent24Hr,
					"vwap24Hr": assets[value].Vwap24Hr,
					"createdAt": time.Now(),
				},
			})



		}



	}

	return "true", nil

}

func getAssets() ([]AssetData, error) {

	session, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c := session.DB("cryptofon").C("assets")

	var results []AssetData
	err = c.Find(nil).All(&results)

	fmt.Println(results)

	return results, nil

}

func getAssetHistory(assetId string, start time.Time, end time.Time) ([]*coincap.AssetHistory, error) {

	client := coincap.NewClient(nil)

	params := &coincap.AssetHistoryRequest{
		Interval: coincap.Day,
		Start:    &coincap.Timestamp{Time: start},
		End:      &coincap.Timestamp{Time: end},
	}
	history, _, err := client.AssetHistoryByID(assetId, params)




	return history, err
}

func addAssetHistory(ObjectId bson.ObjectId, assetId string, history []*coincap.AssetHistory) (bool, error) {

	session, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c := session.DB("cryptofon").C("histories")



	for value := range history {
		fmt.Printf("  %v\n", history[value].PriceUSD)
		fmt.Printf("  %v\n", history[value].Time.UnixNano())


		fmt.Println("aaa")

		var controlHistory, _ = controlAssetHistory(ObjectId, assetId, history[value].Time.UnixNano())

		if controlHistory {
			err = c.Insert(&AssetHistoryData{
				assetId,
				ObjectId,
				history[value].PriceUSD,
				history[value].Time,
				history[value].Time.UnixNano(),
				time.Now(),
			})
			fmt.Println("yokmuş")
		}else {
			fmt.Println("varmıiş")
		}






	}



	return true, nil
}


func controlAssetHistory(ObjectId bson.ObjectId, assetId string, time int64) (bool, error) {

	session, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c := session.DB("cryptofon").C("histories")

	result := AssetHistoryData{}
	err = c.Find(bson.M{"id": assetId, "date": time}).One(&result)
	fmt.Println(assetId)
	fmt.Println(time)
	fmt.Println(result.Id)
	fmt.Println(err)
	if result.Id != "" {
		return false, nil
	}
	return true, nil
}