package main

import (
	"testing"
	"time"
)

func TestListScheduleCyclesUntilDuration(t *testing.T) {
	start := time.Unix(1_800_000_000, 0)
	now := start
	next := listSchedule([]int{100, 120, 150}, 100*time.Second, 350*time.Second, func() time.Time { return now })
	var levels []int
	var holds []time.Duration
	for {
		s, ok := next()
		if !ok {
			break
		}
		levels = append(levels, s.level)
		holds = append(holds, s.hold)
		now = now.Add(s.hold)
	}
	if len(levels) != 4 || levels[0] != 100 || levels[1] != 120 || levels[2] != 150 || levels[3] != 100 {
		t.Fatalf("cycle %v", levels)
	}
	if holds[3] != 50*time.Second {
		t.Fatalf("last hold %s", holds[3])
	}
}

func TestListSchedulePlaysOnceWithoutDuration(t *testing.T) {
	next := listSchedule([]int{50, 80}, time.Second, 0, time.Now)
	a, ok := next()
	b, ok2 := next()
	_, ok3 := next()
	if !ok || !ok2 || ok3 || a.level != 50 || b.level != 80 || a.hold != time.Second {
		t.Fatalf("once: %v %v %v %v", a, ok, b, ok2)
	}
}
