package main

import "testing"

func TestTrackerHost_success(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"https://tracker.example.com:443/announce", "tracker.example.com"},
		{"http://Open.Tracker.org/a", "open.tracker.org"},
		{"udp://udp.example.net:6969", "udp.example.net"},
		{"https://[2001:db8::1]/announce", "2001:db8::1"},
	}
	for _, c := range cases {
		if got := trackerHost(c.in); got != c.want {
			t.Fatalf("trackerHost(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

func TestTrackerHost_failureEmpty(t *testing.T) {
	if got := trackerHost(""); got != unknownTrackerHost {
		t.Fatalf("got %q", got)
	}
	if got := trackerHost("   "); got != unknownTrackerHost {
		t.Fatalf("got %q", got)
	}
}

func TestFirstTrackerHost_prefersParsed(t *testing.T) {
	got := firstTrackerHost([]string{"", "https://a.example/announce", "https://b.example/announce"})
	if got != "a.example" {
		t.Fatalf("got %q", got)
	}
}

func TestPruneTrackers_keepsHottest(t *testing.T) {
	trackers := map[string]*trackerStat{}
	for i := 0; i < maxTrackerHosts+5; i++ {
		host := string(rune('a'+(i%26))) + string(rune('0'+i/26)) + ".t"
		trackers[host] = &trackerStat{Host: host, Up: int64(i), LastHit: int64(i)}
	}
	pruneTrackers(trackers)
	if len(trackers) > maxTrackerHosts {
		t.Fatalf("len=%d", len(trackers))
	}
}

func TestSortedTrackerStats_ordersByVolume(t *testing.T) {
	m := map[string]*trackerStat{
		"b": {Host: "b", Up: 1, Down: 0},
		"a": {Host: "a", Up: 10, Down: 5},
	}
	rows := sortedTrackerStats(m)
	if len(rows) != 2 || rows[0].Host != "a" {
		t.Fatalf("%+v", rows)
	}
}
