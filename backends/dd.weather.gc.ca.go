package backends

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/schachmat/wego/iface"
)

type mscConfig struct {
	lang string
}

// generated with https://www.onlinetool.io/xmltogo/
type siteData struct {
	XMLName                   xml.Name `xml:"siteData"`
	Text                      string   `xml:",chardata"`
	Xsi                       string   `xml:"xsi,attr"`
	NoNamespaceSchemaLocation string   `xml:"noNamespaceSchemaLocation,attr"`
	License                   string   `xml:"license"`
	DateTime                  []struct {
		Text      string `xml:",chardata"`
		Name      string `xml:"name,attr"`
		Zone      string `xml:"zone,attr"`
		UTCOffset string `xml:"UTCOffset,attr"`
		Year      string `xml:"year"`
		Month     struct {
			Text string `xml:",chardata"`
			Name string `xml:"name,attr"`
		} `xml:"month"`
		Day struct {
			Text string `xml:",chardata"`
			Name string `xml:"name,attr"`
		} `xml:"day"`
		Hour        string `xml:"hour"`
		Minute      string `xml:"minute"`
		TimeStamp   string `xml:"timeStamp"`
		TextSummary string `xml:"textSummary"`
	} `xml:"dateTime"`
	Location struct {
		Text      string `xml:",chardata"`
		Continent string `xml:"continent"`
		Country   struct {
			Text string `xml:",chardata"`
			Code string `xml:"code,attr"`
		} `xml:"country"`
		Province struct {
			Text string `xml:",chardata"`
			Code string `xml:"code,attr"`
		} `xml:"province"`
		Name struct {
			Text string `xml:",chardata"`
			Code string `xml:"code,attr"`
			Lat  string `xml:"lat,attr"`
			Lon  string `xml:"lon,attr"`
		} `xml:"name"`
		Region string `xml:"region"`
	} `xml:"location"`
	Warnings          string `xml:"warnings"`
	CurrentConditions struct {
		Text    string `xml:",chardata"`
		Station struct {
			Text string `xml:",chardata"`
			Code string `xml:"code,attr"`
			Lat  string `xml:"lat,attr"`
			Lon  string `xml:"lon,attr"`
		} `xml:"station"`
		DateTime []struct {
			Text      string `xml:",chardata"`
			Name      string `xml:"name,attr"`
			Zone      string `xml:"zone,attr"`
			UTCOffset string `xml:"UTCOffset,attr"`
			Year      string `xml:"year"`
			Month     struct {
				Text string `xml:",chardata"`
				Name string `xml:"name,attr"`
			} `xml:"month"`
			Day struct {
				Text string `xml:",chardata"`
				Name string `xml:"name,attr"`
			} `xml:"day"`
			Hour        string `xml:"hour"`
			Minute      string `xml:"minute"`
			TimeStamp   string `xml:"timeStamp"`
			TextSummary string `xml:"textSummary"`
		} `xml:"dateTime"`
		Condition string `xml:"condition"`
		IconCode  struct {
			Text   string `xml:",chardata"`
			Format string `xml:"format,attr"`
		} `xml:"iconCode"`
		Temperature struct {
			Text     string `xml:",chardata"`
			UnitType string `xml:"unitType,attr"`
			Units    string `xml:"units,attr"`
		} `xml:"temperature"`
		Dewpoint struct {
			Text     string `xml:",chardata"`
			UnitType string `xml:"unitType,attr"`
			Units    string `xml:"units,attr"`
		} `xml:"dewpoint"`
		WindChill struct {
			Text     string `xml:",chardata"`
			UnitType string `xml:"unitType,attr"`
		} `xml:"windChill"`
		Pressure struct {
			Text     string `xml:",chardata"`
			UnitType string `xml:"unitType,attr"`
			Units    string `xml:"units,attr"`
			Change   string `xml:"change,attr"`
			Tendency string `xml:"tendency,attr"`
		} `xml:"pressure"`
		Visibility struct {
			Text     string `xml:",chardata"`
			UnitType string `xml:"unitType,attr"`
			Units    string `xml:"units,attr"`
		} `xml:"visibility"`
		RelativeHumidity struct {
			Text  string `xml:",chardata"`
			Units string `xml:"units,attr"`
		} `xml:"relativeHumidity"`
		Wind struct {
			Text  string `xml:",chardata"`
			Speed struct {
				Text     string `xml:",chardata"`
				UnitType string `xml:"unitType,attr"`
				Units    string `xml:"units,attr"`
			} `xml:"speed"`
			Gust struct {
				Text     string `xml:",chardata"`
				UnitType string `xml:"unitType,attr"`
				Units    string `xml:"units,attr"`
			} `xml:"gust"`
			Direction string `xml:"direction"`
			Bearing   struct {
				Text  string `xml:",chardata"`
				Units string `xml:"units,attr"`
			} `xml:"bearing"`
		} `xml:"wind"`
	} `xml:"currentConditions"`
	ForecastGroup struct {
		Text     string `xml:",chardata"`
		DateTime []struct {
			Text      string `xml:",chardata"`
			Name      string `xml:"name,attr"`
			Zone      string `xml:"zone,attr"`
			UTCOffset string `xml:"UTCOffset,attr"`
			Year      string `xml:"year"`
			Month     struct {
				Text string `xml:",chardata"`
				Name string `xml:"name,attr"`
			} `xml:"month"`
			Day struct {
				Text string `xml:",chardata"`
				Name string `xml:"name,attr"`
			} `xml:"day"`
			Hour        string `xml:"hour"`
			Minute      string `xml:"minute"`
			TimeStamp   string `xml:"timeStamp"`
			TextSummary string `xml:"textSummary"`
		} `xml:"dateTime"`
		RegionalNormals struct {
			Text        string `xml:",chardata"`
			TextSummary string `xml:"textSummary"`
			Temperature []struct {
				Text     string `xml:",chardata"`
				UnitType string `xml:"unitType,attr"`
				Units    string `xml:"units,attr"`
				Class    string `xml:"class,attr"`
			} `xml:"temperature"`
		} `xml:"regionalNormals"`
		Forecast []struct {
			Text   string `xml:",chardata"`
			Period struct {
				Text             string `xml:",chardata"`
				TextForecastName string `xml:"textForecastName,attr"`
			} `xml:"period"`
			TextSummary string `xml:"textSummary"`
			CloudPrecip struct {
				Text        string `xml:",chardata"`
				TextSummary string `xml:"textSummary"`
			} `xml:"cloudPrecip"`
			AbbreviatedForecast struct {
				Text     string `xml:",chardata"`
				IconCode struct {
					Text   string `xml:",chardata"`
					Format string `xml:"format,attr"`
				} `xml:"iconCode"`
				Pop struct {
					Text  string `xml:",chardata"`
					Units string `xml:"units,attr"`
				} `xml:"pop"`
				TextSummary string `xml:"textSummary"`
			} `xml:"abbreviatedForecast"`
			Temperatures struct {
				Text        string `xml:",chardata"`
				TextSummary string `xml:"textSummary"`
				Temperature struct {
					Text     string `xml:",chardata"`
					UnitType string `xml:"unitType,attr"`
					Units    string `xml:"units,attr"`
					Class    string `xml:"class,attr"`
				} `xml:"temperature"`
			} `xml:"temperatures"`
			Winds struct {
				Text        string `xml:",chardata"`
				TextSummary string `xml:"textSummary"`
				Wind        []struct {
					Text  string `xml:",chardata"`
					Index string `xml:"index,attr"`
					Rank  string `xml:"rank,attr"`
					Speed struct {
						Text     string `xml:",chardata"`
						UnitType string `xml:"unitType,attr"`
						Units    string `xml:"units,attr"`
					} `xml:"speed"`
					Gust struct {
						Text     string `xml:",chardata"`
						UnitType string `xml:"unitType,attr"`
						Units    string `xml:"units,attr"`
					} `xml:"gust"`
					Direction string `xml:"direction"`
					Bearing   struct {
						Text  string `xml:",chardata"`
						Units string `xml:"units,attr"`
					} `xml:"bearing"`
				} `xml:"wind"`
			} `xml:"winds"`
			Humidex       string `xml:"humidex"`
			Precipitation struct {
				Text        string `xml:",chardata"`
				TextSummary string `xml:"textSummary"`
				PrecipType  []struct {
					Text  string `xml:",chardata"`
					Start string `xml:"start,attr"`
					End   string `xml:"end,attr"`
				} `xml:"precipType"`
				Accumulation struct {
					Text   string `xml:",chardata"`
					Name   string `xml:"name"`
					Amount struct {
						Text     string `xml:",chardata"`
						UnitType string `xml:"unitType,attr"`
						Units    string `xml:"units,attr"`
					} `xml:"amount"`
				} `xml:"accumulation"`
			} `xml:"precipitation"`
			WindChill struct {
				Text        string `xml:",chardata"`
				TextSummary string `xml:"textSummary"`
				Calculated  []struct {
					Text     string `xml:",chardata"`
					UnitType string `xml:"unitType,attr"`
					Class    string `xml:"class,attr"`
					Index    string `xml:"index,attr"`
				} `xml:"calculated"`
				Frostbite string `xml:"frostbite"`
			} `xml:"windChill"`
			Uv struct {
				Text        string `xml:",chardata"`
				Category    string `xml:"category,attr"`
				Index       string `xml:"index"`
				TextSummary string `xml:"textSummary"`
			} `xml:"uv"`
			RelativeHumidity struct {
				Text  string `xml:",chardata"`
				Units string `xml:"units,attr"`
			} `xml:"relativeHumidity"`
		} `xml:"forecast"`
	} `xml:"forecastGroup"`
	HourlyForecastGroup struct {
		Text     string `xml:",chardata"`
		DateTime []struct {
			Text      string `xml:",chardata"`
			Name      string `xml:"name,attr"`
			Zone      string `xml:"zone,attr"`
			UTCOffset string `xml:"UTCOffset,attr"`
			Year      string `xml:"year"`
			Month     struct {
				Text string `xml:",chardata"`
				Name string `xml:"name,attr"`
			} `xml:"month"`
			Day struct {
				Text string `xml:",chardata"`
				Name string `xml:"name,attr"`
			} `xml:"day"`
			Hour        string `xml:"hour"`
			Minute      string `xml:"minute"`
			TimeStamp   string `xml:"timeStamp"`
			TextSummary string `xml:"textSummary"`
		} `xml:"dateTime"`
		HourlyForecast []struct {
			Text        string `xml:",chardata"`
			DateTimeUTC string `xml:"dateTimeUTC,attr"`
			Condition   string `xml:"condition"`
			IconCode    struct {
				Text   string `xml:",chardata"`
				Format string `xml:"format,attr"`
			} `xml:"iconCode"`
			Temperature struct {
				Text     string `xml:",chardata"`
				UnitType string `xml:"unitType,attr"`
				Units    string `xml:"units,attr"`
			} `xml:"temperature"`
			Lop struct {
				Text     string `xml:",chardata"`
				Category string `xml:"category,attr"`
				Units    string `xml:"units,attr"`
			} `xml:"lop"`
			WindChill struct {
				Text     string `xml:",chardata"`
				UnitType string `xml:"unitType,attr"`
			} `xml:"windChill"`
			Humidex struct {
				Text     string `xml:",chardata"`
				UnitType string `xml:"unitType,attr"`
			} `xml:"humidex"`
			Wind struct {
				Text  string `xml:",chardata"`
				Speed struct {
					Text     string `xml:",chardata"`
					UnitType string `xml:"unitType,attr"`
					Units    string `xml:"units,attr"`
				} `xml:"speed"`
				Direction struct {
					Text        string `xml:",chardata"`
					WindDirFull string `xml:"windDirFull,attr"`
				} `xml:"direction"`
				Gust struct {
					Text     string `xml:",chardata"`
					UnitType string `xml:"unitType,attr"`
					Units    string `xml:"units,attr"`
				} `xml:"gust"`
			} `xml:"wind"`
		} `xml:"hourlyForecast"`
	} `xml:"hourlyForecastGroup"`
	YesterdayConditions struct {
		Text        string `xml:",chardata"`
		Temperature []struct {
			Text     string `xml:",chardata"`
			UnitType string `xml:"unitType,attr"`
			Units    string `xml:"units,attr"`
			Class    string `xml:"class,attr"`
		} `xml:"temperature"`
		Precip struct {
			Text     string `xml:",chardata"`
			UnitType string `xml:"unitType,attr"`
			Units    string `xml:"units,attr"`
		} `xml:"precip"`
	} `xml:"yesterdayConditions"`
	RiseSet struct {
		Text       string `xml:",chardata"`
		Disclaimer string `xml:"disclaimer"`
		DateTime   []struct {
			Text      string `xml:",chardata"`
			Name      string `xml:"name,attr"`
			Zone      string `xml:"zone,attr"`
			UTCOffset string `xml:"UTCOffset,attr"`
			Year      string `xml:"year"`
			Month     struct {
				Text string `xml:",chardata"`
				Name string `xml:"name,attr"`
			} `xml:"month"`
			Day struct {
				Text string `xml:",chardata"`
				Name string `xml:"name,attr"`
			} `xml:"day"`
			Hour        string `xml:"hour"`
			Minute      string `xml:"minute"`
			TimeStamp   string `xml:"timeStamp"`
			TextSummary string `xml:"textSummary"`
		} `xml:"dateTime"`
	} `xml:"riseSet"`
	Almanac struct {
		Text        string `xml:",chardata"`
		Temperature []struct {
			Text     string `xml:",chardata"`
			Class    string `xml:"class,attr"`
			Period   string `xml:"period,attr"`
			UnitType string `xml:"unitType,attr"`
			Units    string `xml:"units,attr"`
			Year     string `xml:"year,attr"`
		} `xml:"temperature"`
		Precipitation []struct {
			Text     string `xml:",chardata"`
			Class    string `xml:"class,attr"`
			Period   string `xml:"period,attr"`
			UnitType string `xml:"unitType,attr"`
			Units    string `xml:"units,attr"`
			Year     string `xml:"year,attr"`
		} `xml:"precipitation"`
		Pop struct {
			Text  string `xml:",chardata"`
			Units string `xml:"units,attr"`
		} `xml:"pop"`
	} `xml:"almanac"`
}

func (c *mscConfig) Setup() {
	flag.StringVar(&c.lang, "msc-lang", "e", "dd.weather.gc.ca backend: the `LANGUAGE` to request from dd.weather.gc.ca (only e and f are supported")
}

func fetchLocation(location string) (lat float64, lon float64, err error) {
	if matched, err := regexp.MatchString(`^-?[0-9]*(\.[0-9]+)?,-?[0-9]*(\.[0-9]+)?$`, location); matched && err == nil {
		s := strings.Split(location, ",")

		if lat, err = strconv.ParseFloat(s[0], 64); err != nil {
			return -1, -1, fmt.Errorf("latitude error: %v", err)
		}

		if lon, err = strconv.ParseFloat(s[1], 64); err != nil {
			return -1, -1, fmt.Errorf("longitude error: %v", err)
		}
	} else {
		return -1, -1, fmt.Errorf("expected location to be only latitude,longitude")
	}

	return lat, lon, nil
}

func fetchNearestStation(lat float64, lon float64) (nearestStationCode string, province string, err error) {
	const URI = "https://dd.meteo.gc.ca/citypage_weather/docs/site_list_towns_en.csv"

	resp, err := http.Get(URI)
	if err != nil {
		return "", "", fmt.Errorf("unable to get (%s) %v", URI, err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("unable to read response body (%s): %v", URI, err)
	}

	bodyReader := bytes.NewReader(body)

	// skip first line
	if firstLine, err := bufio.NewReader(bodyReader).ReadSlice('\n'); err != nil {
		return "", "", err
	} else {
		bodyReader.Seek(int64(len(firstLine)), io.SeekStart)
	}

	minDistance := math.MaxFloat64
	csv := csv.NewReader(bodyReader)
	for {
		record, err := csv.Read()
		if err != io.EOF {
			break
		} else if err != nil {
			return "", "", fmt.Errorf("unable to process the csv at %s: %v", URI, err)
		}

		stationLat, err := strconv.ParseFloat(record[3][:len(record[3])-1], 64)
		if err != nil {
			log.Print(err)
			continue
		}

		stationLon, err := strconv.ParseFloat(record[4][:len(record[4])-1], 64)
		if err != nil {
			log.Print(err)
			continue
		}

		distance := math.Pow(lat-stationLat, 2) + math.Pow(lon-stationLon, 2)
		if distance < minDistance {
			minDistance = distance
			nearestStationCode = record[0]
			province = record[2]
		}
	}

	return nearestStationCode, province, nil
}

func fetchSiteData(stationCode string, province string, lang rune) (*siteData, error) {
	URI := fmt.Sprintf("https://dd.weather.gc.ca/citypage_weather/xml/%s/%s_%c.xml", stationCode, province, lang)

	resp, err := http.Get(URI)
	if err != nil {
		return nil, fmt.Errorf("unable to get (%s) %v", URI, err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read response body (%s): %v", URI, err)
	}

	var data siteData
	if err = xml.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("unable to unmarshal response (%s): %v\nThe json body is: %s", URI, err, string(body))
	}

	return &data, nil
}

func (c *mscConfig) Fetch(location string, numdays int) iface.Data {
	var ret iface.Data

	if lat, lon, err := fetchLocation(location); err != nil {
		log.Fatal(err)
	} else if nearestStationCode, province, err := fetchNearestStation(lat, lon); err != nil {
		log.Fatal(err)
	} else if data, err := fetchSiteData(nearestStationCode, province, rune(c.lang[0])); err != nil {
		log.Fatal(err)
	} else {
		log.Print(data)
	}

	return ret
}

func init() {
	iface.AllBackends["dd.weather.gc.ca"] = &mscConfig{}
}
