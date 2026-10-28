package cachepolicy

import "testing"

func TestColdDefaultKeepsOddIDs(t *testing.T) {
	t.Setenv("HOTEL_COLD_CACHE", "")
	if !Cold() || Hot("1") || !Hot("2") {
		t.Fatalf("default cold=%v hot1=%v hot2=%v", Cold(), Hot("1"), Hot("2"))
	}
	ids := HotIDs()
	if len(ids) != MaxHotelID/2 || ids[0] != "2" || ids[len(ids)-1] != "80" {
		t.Fatalf("default ids %v", ids)
	}
	if HotKey("1_cap") || !HotKey("2_2015-04-10_2015-04-11") {
		t.Fatal("default keys")
	}
}

func TestColdOffCachesEveryID(t *testing.T) {
	for _, value := range []string{"0", "false", "off", "no"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("HOTEL_COLD_CACHE", value)
			if Cold() || !Hot("1") || !Hot("2") || Hot("81") {
				t.Fatalf("cold=%v hot1=%v hot2=%v hot81=%v", Cold(), Hot("1"), Hot("2"), Hot("81"))
			}
			ids := HotIDs()
			if len(ids) != MaxHotelID || ids[0] != "1" || ids[len(ids)-1] != "80" {
				t.Fatalf("ids len %d first %s last %s", len(ids), ids[0], ids[len(ids)-1])
			}
		})
	}
}

func TestColdOn(t *testing.T) {
	for _, value := range []string{"1", "true", "on", "YES"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("HOTEL_COLD_CACHE", value)
			if !Cold() || Hot("3") || !Hot("4") {
				t.Fatalf("value %q cold=%v", value, Cold())
			}
		})
	}
}
