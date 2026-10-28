package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/boot"
	attractions "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/attractions/proto"
	geo "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/geo/proto"
	profile "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/profile/proto"
	rate "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/rate/proto"
	recommendation "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/recommendation/proto"
	reservation "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto"
	review "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/review/proto"
	search "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/search/proto"
	user "github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/user/proto"
	"google.golang.org/grpc"
)

const (
	inDate  = "2015-04-09"
	outDate = "2015-04-10"
	lat     = "37.7867"
	lon     = "-122.4112"

	// Cornell_31 is user i=1: the stored name is Cornell_ plus the hex of "1".
	// The password is that suffix repeated ten times, hashed by the user service.
	username = "Cornell_31"
	password = "1111111111"
)

// Run checks every frontend route and every gRPC method against cfg.
func Run(cfg boot.Config) error {
	r := &report{}
	checkHTTP(r, cfg)
	checkGRPC(r, cfg)
	fmt.Print(r.summary())
	if r.failed > 0 {
		return fmt.Errorf("%d checks failed", r.failed)
	}
	return nil
}

type report struct {
	passed int
	failed int
	lines  []string
}

func (r *report) pass(name string) {
	r.passed++
	r.lines = append(r.lines, "pass  "+name)
	fmt.Println("pass ", name)
}

func (r *report) fail(name, detail string) {
	r.failed++
	r.lines = append(r.lines, "FAIL  "+name+"\n      "+detail)
	fmt.Printf("FAIL  %s\n      %s\n", name, detail)
}

func (r *report) summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n%d passed, %d failed\n", r.passed, r.failed)
	return b.String()
}

func checkHTTP(r *report, cfg boot.Config) {
	base := "http://" + cfg.Services["frontend"]
	login := "username=" + username + "&password=" + password

	httpCheck(r, "GET /", base+"/", 200, func(body []byte) error {
		if !bytes.Contains(body, []byte("<title>Go Microservices Example</title>")) {
			return fmt.Errorf("body missing title: %s", clip(body))
		}
		return nil
	})
	httpCheck(r, "GET /stylesheets/main.css", base+"/stylesheets/main.css", 200, func(body []byte) error {
		if !bytes.Contains(body, []byte("#map")) {
			return fmt.Errorf("stylesheet missing #map: %s", clip(body))
		}
		return nil
	})
	httpCheck(r, "GET /no-such", base+"/no-such", 404, nil)

	httpCheck(r, "GET /hotels missing dates", base+"/hotels", 400, bodyContains("Please specify inDate/outDate params"))
	httpCheck(r, "GET /hotels missing location", base+"/hotels?inDate="+inDate+"&outDate="+outDate, 400, bodyContains("Please specify location params"))
	httpCheck(r, "GET /hotels far away", base+"/hotels?inDate="+inDate+"&outDate="+outDate+"&lat=0&lon=0", 200, func(body []byte) error {
		return expectFeatures(body, map[string]string{})
	})
	httpCheck(r, "GET /hotels nearby", base+"/hotels?inDate="+inDate+"&outDate="+outDate+"&lat="+lat+"&lon="+lon, 200, func(body []byte) error {
		return expectFeatures(body, map[string]string{
			"1": "Clift Hotel",
			"2": "W San Francisco",
			"3": "Hotel Zetta",
		})
	})

	httpCheck(r, "GET /recommendations missing location", base+"/recommendations", 400, bodyContains("Please specify location params"))
	httpCheck(r, "GET /recommendations missing require", base+"/recommendations?lat="+lat+"&lon="+lon, 400, bodyContains("Please specify require params"))
	httpCheck(r, "GET /recommendations rate", base+"/recommendations?lat="+lat+"&lon="+lon+"&require=rate", 200, func(body []byte) error {
		return expectFeatureIDs(body, []string{"9", "24", "39", "54", "69"})
	})
	httpCheck(r, "GET /recommendations price", base+"/recommendations?lat="+lat+"&lon="+lon+"&require=price", 200, func(body []byte) error {
		return expectFeatures(body, map[string]string{"2": "W San Francisco"})
	})
	httpCheck(r, "GET /recommendations distance", base+"/recommendations?lat="+lat+"&lon="+lon+"&require=dis", 200, func(body []byte) error {
		return expectFeatures(body, map[string]string{"1": "Clift Hotel"})
	})

	httpCheck(r, "GET /user missing credentials", base+"/user", 400, bodyContains("Please specify username and password"))
	httpCheck(r, "GET /user login", base+"/user?"+login, 200, jsonMessage("Login successfully!"))
	httpCheck(r, "GET /user bad password", base+"/user?username="+username+"&password=nope", 200, jsonMessage("Failed. Please check your username and password. "))

	httpCheck(r, "GET /review missing credentials", base+"/review", 400, bodyContains("Please specify username and password"))
	httpCheck(r, "GET /review hotel 1", base+"/review?"+login+"&hotelId=1", 200, jsonMessage("Have reviews = 4"))
	httpCheck(r, "GET /review unknown hotel", base+"/review?"+login+"&hotelId=99", 200, jsonMessage("Failed. No Reviews. "))

	httpCheck(r, "GET /restaurants hotel 1", base+"/restaurants?"+login+"&hotelId=1", 200, jsonMessage("Have restaurants = 5"))
	httpCheck(r, "GET /museums hotel 1", base+"/museums?"+login+"&hotelId=1", 200, jsonMessage("Have museums = 1"))
	httpCheck(r, "GET /cinema hotel 1", base+"/cinema?"+login+"&hotelId=1", 200, jsonMessage("Failed. No Cinemas. "))

	reserve := base + "/reservation?inDate=2015-04-11&outDate=2015-04-12&hotelId=1&customerName=Alice&number=1&" + login
	httpCheck(r, "GET /reservation missing dates", base+"/reservation", 400, bodyContains("Please specify inDate/outDate params"))
	httpCheck(r, "GET /reservation bad date", base+"/reservation?inDate=04-11-2015&outDate=2015-04-12&hotelId=1&customerName=Alice&"+login, 400, bodyContains("Please check inDate/outDate format (YYYY-MM-DD)"))
	httpCheck(r, "GET /reservation", reserve, 200, jsonMessage("Reserve successfully!"))
}

func checkGRPC(r *report, cfg boot.Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	dial := func(name string) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, cfg.Services[name], grpc.WithInsecure(), grpc.WithBlock())
	}

	geoConn, err := dial("srv-geo")
	if err != nil {
		r.fail("grpc geo dial", err.Error())
	} else {
		defer geoConn.Close()
		res, err := geo.NewGeoClient(geoConn).Nearby(ctx, &geo.Request{Lat: 37.7867, Lon: -122.4112})
		if err != nil {
			r.fail("grpc Geo.Nearby", err.Error())
		} else if !sameSet(res.HotelIds, []string{"1", "2", "3", "5", "6"}) {
			r.fail("grpc Geo.Nearby", fmt.Sprintf("hotel ids %v", res.HotelIds))
		} else {
			r.pass("grpc Geo.Nearby")
		}
		empty, err := geo.NewGeoClient(geoConn).Nearby(ctx, &geo.Request{Lat: 0, Lon: 0})
		if err != nil {
			r.fail("grpc Geo.Nearby empty", err.Error())
		} else if len(empty.HotelIds) != 0 {
			r.fail("grpc Geo.Nearby empty", fmt.Sprintf("hotel ids %v", empty.HotelIds))
		} else {
			r.pass("grpc Geo.Nearby empty")
		}
	}

	profileConn, err := dial("srv-profile")
	if err != nil {
		r.fail("grpc profile dial", err.Error())
	} else {
		defer profileConn.Close()
		res, err := profile.NewProfileClient(profileConn).GetProfiles(ctx, &profile.Request{HotelIds: []string{"1"}, Locale: "en"})
		if err != nil {
			r.fail("grpc Profile.GetProfiles", err.Error())
		} else if len(res.Hotels) != 1 || res.Hotels[0].GetName() != "Clift Hotel" || res.Hotels[0].GetPhoneNumber() != "(415) 775-4700" {
			r.fail("grpc Profile.GetProfiles", fmt.Sprintf("%v", res.Hotels))
		} else {
			r.pass("grpc Profile.GetProfiles")
		}
	}

	rateConn, err := dial("srv-rate")
	if err != nil {
		r.fail("grpc rate dial", err.Error())
	} else {
		defer rateConn.Close()
		res, err := rate.NewRateClient(rateConn).GetRates(ctx, &rate.Request{
			HotelIds: []string{"1"}, InDate: inDate, OutDate: outDate,
		})
		if err != nil {
			r.fail("grpc Rate.GetRates", err.Error())
		} else if len(res.RatePlans) != 1 || res.RatePlans[0].GetHotelId() != "1" || res.RatePlans[0].GetCode() != "RACK" || res.RatePlans[0].GetRoomType().GetBookableRate() != 109 {
			r.fail("grpc Rate.GetRates", fmt.Sprintf("%v", res.RatePlans))
		} else {
			r.pass("grpc Rate.GetRates")
		}
	}

	recConn, err := dial("srv-recommendation")
	if err != nil {
		r.fail("grpc recommendation dial", err.Error())
	} else {
		defer recConn.Close()
		client := recommendation.NewRecommendationClient(recConn)
		res, err := client.GetRecommendations(ctx, &recommendation.Request{Require: "rate", Lat: 37.7867, Lon: -122.4112})
		if err != nil {
			r.fail("grpc Recommendation.GetRecommendations rate", err.Error())
		} else if !sameSet(res.HotelIds, []string{"9", "24", "39", "54", "69"}) {
			r.fail("grpc Recommendation.GetRecommendations rate", fmt.Sprintf("%v", res.HotelIds))
		} else {
			r.pass("grpc Recommendation.GetRecommendations rate")
		}
		res, err = client.GetRecommendations(ctx, &recommendation.Request{Require: "price", Lat: 37.7867, Lon: -122.4112})
		if err != nil {
			r.fail("grpc Recommendation.GetRecommendations price", err.Error())
		} else if !sameSet(res.HotelIds, []string{"2"}) {
			r.fail("grpc Recommendation.GetRecommendations price", fmt.Sprintf("%v", res.HotelIds))
		} else {
			r.pass("grpc Recommendation.GetRecommendations price")
		}
		res, err = client.GetRecommendations(ctx, &recommendation.Request{Require: "dis", Lat: 37.7867, Lon: -122.4112})
		if err != nil {
			r.fail("grpc Recommendation.GetRecommendations distance", err.Error())
		} else if !sameSet(res.HotelIds, []string{"1"}) {
			r.fail("grpc Recommendation.GetRecommendations distance", fmt.Sprintf("%v", res.HotelIds))
		} else {
			r.pass("grpc Recommendation.GetRecommendations distance")
		}
	}

	userConn, err := dial("srv-user")
	if err != nil {
		r.fail("grpc user dial", err.Error())
	} else {
		defer userConn.Close()
		client := user.NewUserClient(userConn)
		res, err := client.CheckUser(ctx, &user.Request{Username: username, Password: password})
		if err != nil {
			r.fail("grpc User.CheckUser", err.Error())
		} else if !res.Correct {
			r.fail("grpc User.CheckUser", "correct=false")
		} else {
			r.pass("grpc User.CheckUser")
		}
		res, err = client.CheckUser(ctx, &user.Request{Username: username, Password: "nope"})
		if err != nil {
			r.fail("grpc User.CheckUser reject", err.Error())
		} else if res.Correct {
			r.fail("grpc User.CheckUser reject", "correct=true")
		} else {
			r.pass("grpc User.CheckUser reject")
		}
	}

	revConn, err := dial("srv-review")
	if err != nil {
		r.fail("grpc review dial", err.Error())
	} else {
		defer revConn.Close()
		res, err := review.NewReviewClient(revConn).GetReviews(ctx, &review.Request{HotelId: "1"})
		if err != nil {
			r.fail("grpc Review.GetReviews", err.Error())
		} else if len(res.Reviews) != 4 {
			r.fail("grpc Review.GetReviews", fmt.Sprintf("len %d", len(res.Reviews)))
		} else {
			r.pass("grpc Review.GetReviews")
		}
	}

	resConn, err := dial("srv-reservation")
	if err != nil {
		r.fail("grpc reservation dial", err.Error())
	} else {
		defer resConn.Close()
		client := reservation.NewReservationClient(resConn)
		res, err := client.CheckAvailability(ctx, &reservation.Request{
			HotelId: []string{"1", "2"}, InDate: inDate, OutDate: outDate, RoomNumber: 1,
		})
		if err != nil {
			r.fail("grpc Reservation.CheckAvailability", err.Error())
		} else if !sameSet(res.HotelId, []string{"1", "2"}) {
			r.fail("grpc Reservation.CheckAvailability", fmt.Sprintf("%v", res.HotelId))
		} else {
			r.pass("grpc Reservation.CheckAvailability")
		}
		res, err = client.MakeReservation(ctx, &reservation.Request{
			CustomerName: "Alice", HotelId: []string{"5"}, InDate: "2015-04-11", OutDate: "2015-04-12", RoomNumber: 1,
		})
		if err != nil {
			r.fail("grpc Reservation.MakeReservation", err.Error())
		} else if !sameSet(res.HotelId, []string{"5"}) {
			r.fail("grpc Reservation.MakeReservation", fmt.Sprintf("%v", res.HotelId))
		} else {
			r.pass("grpc Reservation.MakeReservation")
		}
	}

	attrConn, err := dial("srv-attractions")
	if err != nil {
		r.fail("grpc attractions dial", err.Error())
	} else {
		defer attrConn.Close()
		client := attractions.NewAttractionsClient(attrConn)
		res, err := client.NearbyRest(ctx, &attractions.Request{HotelId: "1"})
		if err != nil {
			r.fail("grpc Attractions.NearbyRest", err.Error())
		} else if !sameSet(res.AttractionIds, []string{"1", "2", "3", "4", "5"}) {
			r.fail("grpc Attractions.NearbyRest", fmt.Sprintf("%v", res.AttractionIds))
		} else {
			r.pass("grpc Attractions.NearbyRest")
		}
		res, err = client.NearbyMus(ctx, &attractions.Request{HotelId: "1"})
		if err != nil {
			r.fail("grpc Attractions.NearbyMus", err.Error())
		} else if !sameSet(res.AttractionIds, []string{"4"}) {
			r.fail("grpc Attractions.NearbyMus", fmt.Sprintf("%v", res.AttractionIds))
		} else {
			r.pass("grpc Attractions.NearbyMus")
		}
		res, err = client.NearbyCinema(ctx, &attractions.Request{HotelId: "1"})
		if err != nil {
			r.fail("grpc Attractions.NearbyCinema", err.Error())
		} else if len(res.AttractionIds) != 0 {
			r.fail("grpc Attractions.NearbyCinema", fmt.Sprintf("%v", res.AttractionIds))
		} else {
			r.pass("grpc Attractions.NearbyCinema")
		}
	}

	searchConn, err := dial("srv-search")
	if err != nil {
		r.fail("grpc search dial", err.Error())
	} else {
		defer searchConn.Close()
		res, err := search.NewSearchClient(searchConn).Nearby(ctx, &search.NearbyRequest{
			Lat: 37.7867, Lon: -122.4112, InDate: inDate, OutDate: outDate,
		})
		if err != nil {
			r.fail("grpc Search.Nearby", err.Error())
		} else if !sameSet(res.HotelIds, []string{"1", "2", "3"}) {
			r.fail("grpc Search.Nearby", fmt.Sprintf("%v", res.HotelIds))
		} else {
			r.pass("grpc Search.Nearby")
		}
	}
}

func httpCheck(r *report, name, url string, status int, check func([]byte) error) {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		r.fail(name, err.Error())
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		r.fail(name, err.Error())
		return
	}
	if resp.StatusCode != status {
		r.fail(name, fmt.Sprintf("status %d, want %d, body %s", resp.StatusCode, status, clip(body)))
		return
	}
	if check != nil {
		if err := check(body); err != nil {
			r.fail(name, err.Error())
			return
		}
	}
	r.pass(name)
}

func bodyContains(text string) func([]byte) error {
	return func(body []byte) error {
		if !bytes.Contains(body, []byte(text)) {
			return fmt.Errorf("body %s", clip(body))
		}
		return nil
	}
}

func jsonMessage(want string) func([]byte) error {
	return func(body []byte) error {
		var msg struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(body, &msg); err != nil {
			return fmt.Errorf("json %s: %v", clip(body), err)
		}
		if msg.Message != want {
			return fmt.Errorf("message %q, want %q", msg.Message, want)
		}
		return nil
	}
}

type featureCollection struct {
	Type     string `json:"type"`
	Features []struct {
		Type       string `json:"type"`
		ID         string `json:"id"`
		Properties struct {
			Name  string `json:"name"`
			Phone string `json:"phone_number"`
		} `json:"properties"`
		Geometry struct {
			Type        string    `json:"type"`
			Coordinates []float64 `json:"coordinates"`
		} `json:"geometry"`
	} `json:"features"`
}

func parseFeatures(body []byte) (featureCollection, error) {
	var fc featureCollection
	if err := json.Unmarshal(body, &fc); err != nil {
		return fc, fmt.Errorf("json %s: %v", clip(body), err)
	}
	if fc.Type != "FeatureCollection" {
		return fc, fmt.Errorf("type %q", fc.Type)
	}
	for _, f := range fc.Features {
		if f.Type != "Feature" || f.Geometry.Type != "Point" || len(f.Geometry.Coordinates) != 2 {
			return fc, fmt.Errorf("feature %+v", f)
		}
	}
	return fc, nil
}

func expectFeatures(body []byte, names map[string]string) error {
	fc, err := parseFeatures(body)
	if err != nil {
		return err
	}
	if len(fc.Features) != len(names) {
		return fmt.Errorf("got %d features, want %d (%s)", len(fc.Features), len(names), clip(body))
	}
	got := map[string]string{}
	for _, f := range fc.Features {
		got[f.ID] = f.Properties.Name
	}
	for id, name := range names {
		if got[id] != name {
			return fmt.Errorf("id %s name %q, want %q", id, got[id], name)
		}
	}
	return nil
}

func expectFeatureIDs(body []byte, ids []string) error {
	fc, err := parseFeatures(body)
	if err != nil {
		return err
	}
	got := make([]string, 0, len(fc.Features))
	for _, f := range fc.Features {
		if f.Properties.Name == "" || f.Properties.Phone == "" {
			return fmt.Errorf("feature %s missing name or phone", f.ID)
		}
		got = append(got, f.ID)
	}
	if !sameSet(got, ids) {
		return fmt.Errorf("ids %v, want %v", got, ids)
	}
	return nil
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	a := append([]string(nil), got...)
	b := append([]string(nil), want...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func clip(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}
