package main

import (
	"encoding/xml"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const SERVER = "localhost"
const DBNAME = "currencydb"
const COLLECTION = "rates"

type Item struct {
	Currency string  `bson:"currency" json:"currency"`
	Rate     float32 `bson:"rate" json:"rate"`
}

type Rate struct {
	ID       bson.ObjectId `bson:"_id" json:"id"`
	RateDate string        `bson:"rate_date" json:"rateDate"`
	Rates    []*Item       `bson:"rates" json:"rates"`
}

type AnalyzeRes struct {
	Currency string  `bson:"_id" json:"Currency"`
	Max      float32 `bson:"max" json:"max"`
	Min      float32 `bson:"min" json:"min"`
	Avg      float32 `bson:"avg" json:"avg"`
}

type DailyRate struct {
	Base  string             `json:"base"`
	Rates map[string]float32 `json:"rates"`
}

type RateAnalysisRes struct {
	Base  string                   `json:"base"`
	Rates map[string]*AnalysisData `json:"rates_analyze"`
}

type AnalysisData struct {
	Min float32 `json:"min"`
	Max float32 `json:"max"`
	Avg float32 `json:"avg"`
}

type DB struct{}

var db *mgo.Database
var p = &DB{}

func (p *DB) Connect() {
	session, err := mgo.Dial(SERVER)
	if err != nil {
		log.Fatal(err)
	}
	db = session.DB(DBNAME)
}

func (p *DB) FindAll() ([]Rate, error) {
	var rates []Rate
	err := db.C(COLLECTION).Find(nil).All(&rates)
	return rates, err
}

func (p *DB) FindById(id string) (Rate, error) {
	var rate Rate
	err := db.C(COLLECTION).FindId(bson.ObjectIdHex(id)).One(&rate)
	return rate, err
}

func (p *DB) GetLatest() (Rate, error) {
	var rate Rate
	err := db.C(COLLECTION).Find(nil).Sort("-rate_date").One(&rate)
	return rate, err
}

func (p *DB) FindByDate(date string) (*Rate, error) {
	var rate Rate
	err := db.C(COLLECTION).Find(bson.M{"rate_date": date}).One(&rate)
	return &rate, err
}

func (p *DB) Analyze() ([]*AnalyzeRes, error) {
	pipe := db.C(COLLECTION).Pipe([]bson.M{
		{"$unwind": "$rates"},
		{"$project": bson.M{
			"_id":       1,
			"rate_date": 1,
			"currency":  "$rates.currency",
			"rate":      "$rates.rate",
		}},
		{"$group": bson.M{
			"_id": "$currency",
			"max": bson.M{"$max": "$rate"},
			"min": bson.M{"$min": "$rate"},
			"sum": bson.M{"$sum": "$rate"},
			"avg": bson.M{"$avg": "$rate"},
		}},
		{
			"$sort": bson.M{"_id": 1},
		},
	})
	res := []*AnalyzeRes{}
	err := pipe.All(&res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (p *DB) Save(rate *Rate) error {
	oldRate, err := p.FindByDate(rate.RateDate)
	if err != nil || oldRate == nil {
		rate.ID = bson.NewObjectId()
		err = p.Insert(rate)
	} else {
		rate.ID = oldRate.ID
		err = p.Update(rate)
	}
	return err
}

func (p *DB) Insert(rate *Rate) error {
	err := db.C(COLLECTION).Insert(rate)
	return err
}

func (p *DB) Update(rate *Rate) error {
	err := db.C(COLLECTION).UpdateId(rate.ID, rate)
	return err
}

func initServer() {
	client := http.Client{}

	req, err := http.NewRequest("GET", "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml", nil)

	if err != nil {
		log.Fatal(err)
	}

	resp, err := client.Do(req)

	if err != nil {
		log.Fatal(err)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	type Cube struct {
		Currency string  `xml:"currency,attr"`
		Rate     float32 `xml:"rate,attr"`
	}

	type CubeDate struct {
		Time  string  `xml:"time,attr"`
		Cubes []*Cube `xml:"Cube"`
	}

	type Response struct {
		CubeDates []*CubeDate `xml:"Cube>Cube"`
	}

	var response Response
	err = xml.Unmarshal(respBody, &response)
	if err != nil {
		log.Fatal(err)
	}

	for _, cube := range response.CubeDates {
		items := []*Item{}
		for _, c := range cube.Cubes {
			items = append(items, &Item{
				Currency: c.Currency,
				Rate:     c.Rate,
			})
		}

		rate := &Rate{
			RateDate: cube.Time,
			Rates:    items,
		}

		if err := p.Save(rate); err != nil {
			log.Fatal(err)
		}
	}
}

func getLatest(c echo.Context) error {
	r, err := p.GetLatest()
	if err != nil {
		log.Println("LatestRateEndPoint, error on GetLatest", err)
		return c.JSON(http.StatusBadRequest, nil)
	}

	rates := map[string]float32{}
	for _, item := range r.Rates {
		rates[item.Currency] = item.Rate
	}

	res := &DailyRate{
		Base:  "EUR",
		Rates: rates,
	}

	return c.JSON(http.StatusOK, res)
}

func getAnalyze(c echo.Context) error {
	analyze, err := p.Analyze()
	if err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	res := &RateAnalysisRes{
		Base:  "EUR",
		Rates: map[string]*AnalysisData{},
	}

	for _, rate := range analyze {
		data := &AnalysisData{
			Min: rate.Min,
			Max: rate.Max,
			Avg: rate.Avg,
		}
		res.Rates[rate.Currency] = data
	}

	return c.JSON(http.StatusOK, res)
}

func getDateRate(c echo.Context) error {
	date := c.Param("date")
	rate, err := p.FindByDate(date)
	if err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	rates := map[string]float32{}
	for _, item := range rate.Rates {
		rates[item.Currency] = item.Rate
	}

	res := &DailyRate{
		Base:  "EUR",
		Rates: rates,
	}

	return c.JSON(http.StatusOK, res)
}

func main() {
	p.Connect()

	initServer()

	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Routes
	e.GET("/rates/latest", getLatest)
	e.GET("/rates/analyze", getAnalyze)
	e.GET("/rates/:date", getDateRate)

	// Start server
	e.Logger.Fatal(e.Start(":3000"))
}
