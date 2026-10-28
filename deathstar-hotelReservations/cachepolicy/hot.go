// Package cachepolicy splits hotel ids into a hot half served from memcached
// and a cold half read from MongoDB on every request.
//
// Geo returns the five nearest hotels, and those ids are consecutive, so a
// search contains both parities. Even ids are written to memcached. Odd ids
// are not, which keeps the database on the request path.
//
// HOTEL_COLD_CACHE turns that split off. Unset, 1, true, or on keeps the odd
// ids on MongoDB. 0, false, off, or no caches every seeded hotel id.
package cachepolicy

import (
	"os"
	"strconv"
	"strings"
)

// MaxHotelID is the last id seeded by the hotel databases.
const MaxHotelID = 80

// Cold reports whether odd hotel ids stay out of memcached.
func Cold() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("HOTEL_COLD_CACHE"))) {
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

// Hot reports whether hotelID is served from memcached.
func Hot(hotelID string) bool {
	n, err := strconv.Atoi(hotelID)
	if err != nil || n <= 0 || n > MaxHotelID {
		return false
	}
	if !Cold() {
		return true
	}
	return n%2 == 0
}

// HotKey reports whether a memcached key belongs to a hot hotel.
// Keys are "12", "12_cap", or "12_2015-04-10_2015-04-11".
func HotKey(key string) bool {
	id := key
	if i := strings.IndexByte(key, '_'); i >= 0 {
		id = key[:i]
	}
	return Hot(id)
}

// HotIDs lists the hotel ids warmed into memcached, in seed order.
func HotIDs() []string {
	start, step := 2, 2
	if !Cold() {
		start, step = 1, 1
	}
	ids := make([]string, 0, MaxHotelID/step)
	for i := start; i <= MaxHotelID; i += step {
		ids = append(ids, strconv.Itoa(i))
	}
	return ids
}
