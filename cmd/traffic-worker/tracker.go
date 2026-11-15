package main

import (
	"net/url"
	"sort"
	"strings"
)

const (
	maxTrackerHosts       = 128
	maxTrackerLookupsScan = 40
	maxHashState          = 2000
	unknownTrackerHost    = "unknown"
)

type trackerStat struct {
	Host    string `json:"host"`
	Up      int64  `json:"up"`
	Down    int64  `json:"down"`
	LastHit int64  `json:"lastHit"`
}

type hashState struct {
	Up   int64  `json:"up"`
	Down int64  `json:"down"`
	Host string `json:"host"`
}

// trackerHost extracts the announce host (best-effort) for attribution.
func trackerHost(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return unknownTrackerHost
	}
	u, err := url.Parse(rawURL)
	if err == nil {
		host := strings.ToLower(strings.TrimSpace(u.Hostname()))
		if host != "" {
			return host
		}
		// udp://host:port without standard parse on some inputs
		if u.Host != "" {
			h := u.Host
			if i := strings.IndexByte(h, ':'); i >= 0 {
				h = h[:i]
			}
			h = strings.ToLower(strings.TrimSpace(h))
			if h != "" {
				return h
			}
		}
	}
	// Fallback: strip scheme and path manually.
	s := rawURL
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	if i := strings.IndexAny(s, "/?"); i >= 0 {
		s = s[:i]
	}
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return unknownTrackerHost
	}
	return s
}

func firstTrackerHost(urls []string) string {
	for _, u := range urls {
		h := trackerHost(u)
		if h != unknownTrackerHost {
			return h
		}
	}
	if len(urls) > 0 {
		return trackerHost(urls[0])
	}
	return unknownTrackerHost
}

func pruneHashState(state map[string]hashState, alive map[string]struct{}) {
	for hash := range state {
		if _, ok := alive[hash]; !ok {
			delete(state, hash)
		}
	}
	if len(state) <= maxHashState {
		return
	}
	// Drop arbitrary extras (map iteration order) until under cap.
	n := len(state) - maxHashState
	for hash := range state {
		if n <= 0 {
			break
		}
		delete(state, hash)
		n--
	}
}

func pruneTrackers(trackers map[string]*trackerStat) {
	if len(trackers) <= maxTrackerHosts {
		return
	}
	type pair struct {
		host string
		hit  int64
	}
	rows := make([]pair, 0, len(trackers))
	for host, st := range trackers {
		hit := int64(0)
		if st != nil {
			hit = st.LastHit
		}
		rows = append(rows, pair{host: host, hit: hit})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].hit < rows[j].hit })
	drop := len(trackers) - maxTrackerHosts
	for i := 0; i < drop; i++ {
		delete(trackers, rows[i].host)
	}
}

func sortedTrackerStats(trackers map[string]*trackerStat) []trackerStat {
	out := make([]trackerStat, 0, len(trackers))
	for host, st := range trackers {
		if st == nil {
			continue
		}
		row := *st
		row.Host = host
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		ti, tj := out[i].Up+out[i].Down, out[j].Up+out[j].Down
		if ti == tj {
			return out[i].Host < out[j].Host
		}
		return ti > tj
	})
	return out
}

func addTrackerDelta(trackers map[string]*trackerStat, host string, up, down, now int64) {
	if host == "" {
		host = unknownTrackerHost
	}
	st := trackers[host]
	if st == nil {
		st = &trackerStat{Host: host}
		trackers[host] = st
	}
	st.Up += up
	st.Down += down
	st.LastHit = now
}
