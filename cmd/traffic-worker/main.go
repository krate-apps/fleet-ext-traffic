// Command traffic-worker collects global and per-tracker upload/download traffic buckets.
package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type settings struct {
	Enabled     bool `json:"enabled"`
	IntervalMin int  `json:"intervalMin"`
}

// buckets mirrors ruTorrent rStat: rolling hour/day/month windows with hit times.
type buckets struct {
	HourUp       []int64 `json:"hourUp"`
	HourDown     []int64 `json:"hourDown"`
	HourHit      []int64 `json:"hourHit"`
	WeekUp       []int64 `json:"weekUp"`
	WeekDown     []int64 `json:"weekDown"`
	WeekHit      []int64 `json:"weekHit"`
	DayUp        []int64 `json:"dayUp"`
	DayDown      []int64 `json:"dayDown"`
	DayHit       []int64 `json:"dayHit"`
	MonthUp      []int64 `json:"monthUp"`
	MonthDown    []int64 `json:"monthDown"`
	MonthHit     []int64 `json:"monthHit"`
	LastUp       int64   `json:"lastUp"`
	LastDown     int64   `json:"lastDown"`
	LastCollect  int64   `json:"lastCollect"`
	Bootstrapped bool    `json:"bootstrapped"`
}

const weekSlots = 168 // 7d × 24h rolling hourly points

type torrentRow struct {
	Hash      string `json:"hash"`
	UpTotal   int64  `json:"upTotal"`
	DownTotal int64  `json:"downTotal"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	go loopCollect()

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := append([]byte(nil), in.Bytes()...)
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		// Handle on a goroutine so hostCall can receive replies on stdin (same pattern as rules/hooks).
		go handleRequest(req)
	}
}

func handleRequest(req request) {
	switch req.Method {
	case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
		reply(req.ID, map[string]any{"ok": true})
	case "fleet.shutdown":
		reply(req.ID, map[string]any{"ok": true})
		os.Exit(0)
	case "ext.traffic.getSettings":
		st, err := loadSettings()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, st)
	case "ext.traffic.saveSettings":
		var st settings
		if err := json.Unmarshal(req.Params, &st); err != nil {
			replyErr(req.ID, "invalid settings")
			return
		}
		if err := validateSettings(st); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		if err := saveSettings(&st); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, st)
	case "ext.traffic.get":
		var p struct {
			Mode string `json:"mode"` // day|month|year
		}
		_ = json.Unmarshal(req.Params, &p)
		res, err := getSeries(p.Mode)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	case "ext.traffic.getTrackers":
		res, err := getTrackers()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	case "ext.traffic.export":
		var p struct {
			Format string `json:"format"` // json|csv
			Scope  string `json:"scope"`  // global|trackers|all
		}
		_ = json.Unmarshal(req.Params, &p)
		res, err := exportStats(p.Format, p.Scope)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	case "ext.traffic.scan":
		res, err := collectOnce(true)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	case "ext.traffic.clear":
		if err := saveBuckets(emptyBuckets()); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		if err := saveTrackers(map[string]*trackerStat{}); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		if err := saveHashState(map[string]hashState{}); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{"ok": true})
	default:
		replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func loopCollect() {
	time.Sleep(4 * time.Second)
	for {
		st, err := loadSettings()
		interval := 15
		if err == nil && st.IntervalMin > 0 {
			interval = st.IntervalMin
		}
		if err == nil && st.Enabled {
			_, _ = collectOnce(false)
		}
		time.Sleep(time.Duration(interval) * time.Minute)
	}
}

func validateSettings(st settings) error {
	allowed := map[int]struct{}{1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {}, 10: {}, 12: {}, 15: {}, 20: {}, 30: {}, 60: {}}
	if _, ok := allowed[st.IntervalMin]; !ok {
		return fmt.Errorf("intervalMin must be one of 1,2,3,4,5,6,10,12,15,20,30,60")
	}
	return nil
}

func defaultSettings() settings {
	return settings{Enabled: true, IntervalMin: 15}
}

func emptyBuckets() *buckets {
	return &buckets{
		HourUp: make([]int64, 24), HourDown: make([]int64, 24), HourHit: make([]int64, 24),
		WeekUp: make([]int64, weekSlots), WeekDown: make([]int64, weekSlots), WeekHit: make([]int64, weekSlots),
		DayUp: make([]int64, 31), DayDown: make([]int64, 31), DayHit: make([]int64, 31),
		MonthUp: make([]int64, 12), MonthDown: make([]int64, 12), MonthHit: make([]int64, 12),
	}
}

func collectOnce(force bool) (map[string]any, error) {
	st, err := loadSettings()
	if err != nil {
		return nil, err
	}
	if !st.Enabled && !force {
		return map[string]any{"ok": true, "skipped": true, "reason": "disabled"}, nil
	}

	raw, err := hostCall("fleet.torrents.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Torrents []torrentRow `json:"torrents"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, err
	}

	b, err := loadBuckets()
	if err != nil {
		return nil, err
	}
	trackers, err := loadTrackers()
	if err != nil {
		return nil, err
	}
	prev, err := loadHashState()
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	alive := map[string]struct{}{}
	var up, down int64
	type deltaRow struct {
		hash string
		up   int64
		down int64
		host string
	}
	deltas := make([]deltaRow, 0, len(wrap.Torrents))

	for _, t := range wrap.Torrents {
		hash := strings.ToUpper(strings.TrimSpace(t.Hash))
		up += t.UpTotal
		down += t.DownTotal
		if hash == "" {
			continue
		}
		alive[hash] = struct{}{}
		old, ok := prev[hash]
		host := old.Host
		if !ok {
			prev[hash] = hashState{Up: t.UpTotal, Down: t.DownTotal, Host: host}
			continue
		}
		dUp := t.UpTotal - old.Up
		dDown := t.DownTotal - old.Down
		if dUp < 0 {
			dUp = 0
		}
		if dDown < 0 {
			dDown = 0
		}
		prev[hash] = hashState{Up: t.UpTotal, Down: t.DownTotal, Host: host}
		if dUp == 0 && dDown == 0 {
			continue
		}
		deltas = append(deltas, deltaRow{hash: hash, up: dUp, down: dDown, host: host})
	}

	if !b.Bootstrapped {
		b.LastUp = up
		b.LastDown = down
		b.LastCollect = now
		b.Bootstrapped = true
		pruneHashState(prev, alive)
		if err := saveBuckets(b); err != nil {
			return nil, err
		}
		if err := saveHashState(prev); err != nil {
			return nil, err
		}
		return map[string]any{"ok": true, "bootstrapped": true, "up": up, "down": down, "torrents": len(wrap.Torrents)}, nil
	}

	// Resolve missing tracker hosts for torrents with positive delta (capped lookups).
	needLookup := make([]deltaRow, 0)
	for _, d := range deltas {
		if strings.TrimSpace(d.host) == "" {
			needLookup = append(needLookup, d)
		}
	}
	// Prefer larger deltas first.
	for i := 0; i < len(needLookup); i++ {
		for j := i + 1; j < len(needLookup); j++ {
			vi := needLookup[i].up + needLookup[i].down
			vj := needLookup[j].up + needLookup[j].down
			if vj > vi {
				needLookup[i], needLookup[j] = needLookup[j], needLookup[i]
			}
		}
	}
	lookups := 0
	for _, d := range needLookup {
		if lookups >= maxTrackerLookupsScan {
			break
		}
		host := resolveTorrentTrackerHost(d.hash)
		lookups++
		hs := prev[d.hash]
		hs.Host = host
		prev[d.hash] = hs
		for i := range deltas {
			if deltas[i].hash == d.hash {
				deltas[i].host = host
			}
		}
	}

	var deltaUp, deltaDown int64
	attributed := 0
	for _, d := range deltas {
		deltaUp += d.up
		deltaDown += d.down
		host := strings.TrimSpace(d.host)
		if host == "" {
			host = unknownTrackerHost
		}
		addTrackerDelta(trackers, host, d.up, d.down, now)
		attributed++
	}

	// Keep global series consistent with prior behavior (session totals delta).
	gUp := up - b.LastUp
	gDown := down - b.LastDown
	if gUp < 0 {
		gUp = 0
	}
	if gDown < 0 {
		gDown = 0
	}
	if gUp == 0 && gDown == 0 {
		gUp, gDown = deltaUp, deltaDown
	}

	t := time.Unix(now, 0).Local()
	hour := t.Hour()
	day := t.Day() - 1
	if day < 0 {
		day = 0
	}
	if day > 30 {
		day = 30
	}
	month := int(t.Month()) - 1

	ensureLen(&b.HourUp, 24)
	ensureLen(&b.HourDown, 24)
	ensureLen(&b.HourHit, 24)
	ensureLen(&b.WeekUp, weekSlots)
	ensureLen(&b.WeekDown, weekSlots)
	ensureLen(&b.WeekHit, weekSlots)
	ensureLen(&b.DayUp, 31)
	ensureLen(&b.DayDown, 31)
	ensureLen(&b.DayHit, 31)
	ensureLen(&b.MonthUp, 12)
	ensureLen(&b.MonthDown, 12)
	ensureLen(&b.MonthHit, 12)

	clearStaleHours(b, t)
	clearStaleWeek(b, t)
	clearStaleDays(b, t)
	clearStaleMonths(b, t)

	b.HourUp[hour] += gUp
	b.HourDown[hour] += gDown
	b.HourHit[hour] = now
	widx := weekIndex(t)
	b.WeekUp[widx] += gUp
	b.WeekDown[widx] += gDown
	b.WeekHit[widx] = now
	b.DayUp[day] += gUp
	b.DayDown[day] += gDown
	b.DayHit[day] = now
	b.MonthUp[month] += gUp
	b.MonthDown[month] += gDown
	b.MonthHit[month] = now

	b.LastUp = up
	b.LastDown = down
	b.LastCollect = now

	pruneHashState(prev, alive)
	pruneTrackers(trackers)

	if err := saveBuckets(b); err != nil {
		return nil, err
	}
	if err := saveTrackers(trackers); err != nil {
		return nil, err
	}
	if err := saveHashState(prev); err != nil {
		return nil, err
	}
	return map[string]any{
		"ok":           true,
		"deltaUp":      gUp,
		"deltaDown":    gDown,
		"up":           up,
		"down":         down,
		"torrents":     len(wrap.Torrents),
		"trackerHits":  attributed,
		"trackerHosts": len(trackers),
		"lookups":      lookups,
		"at":           time.Unix(now, 0).UTC().Format(time.RFC3339),
		"note":         "per-tracker attribution uses interval sampling (trackers.list, capped lookups/scan)",
	}, nil
}

func resolveTorrentTrackerHost(hash string) string {
	raw, err := hostCall("fleet.trackers.list", map[string]any{"hash": hash})
	if err != nil {
		return unknownTrackerHost
	}
	var payload struct {
		Trackers []struct {
			URL     string `json:"url"`
			Enabled bool   `json:"enabled"`
		} `json:"trackers"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return unknownTrackerHost
	}
	urls := make([]string, 0, len(payload.Trackers))
	for _, tr := range payload.Trackers {
		if !tr.Enabled && len(payload.Trackers) > 1 {
			continue
		}
		if u := strings.TrimSpace(tr.URL); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		for _, tr := range payload.Trackers {
			if u := strings.TrimSpace(tr.URL); u != "" {
				urls = append(urls, u)
			}
		}
	}
	return firstTrackerHost(urls)
}

func weekIndex(t time.Time) int {
	h := int(t.Unix() / 3600)
	mod := h % weekSlots
	if mod < 0 {
		mod += weekSlots
	}
	return mod
}

func clearStaleWeek(b *buckets, now time.Time) {
	cutoff := now.Add(-7 * 24 * time.Hour).Unix()
	ensureLen(&b.WeekUp, weekSlots)
	ensureLen(&b.WeekDown, weekSlots)
	ensureLen(&b.WeekHit, weekSlots)
	for i := 0; i < weekSlots; i++ {
		if b.WeekHit[i] > 0 && b.WeekHit[i] < cutoff {
			b.WeekUp[i] = 0
			b.WeekDown[i] = 0
			b.WeekHit[i] = 0
		}
	}
}

func clearStaleHours(b *buckets, now time.Time) {
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).Unix()
	for i := 0; i < 24; i++ {
		if b.HourHit[i] > 0 && b.HourHit[i] < today {
			b.HourUp[i] = 0
			b.HourDown[i] = 0
			b.HourHit[i] = 0
		}
	}
}

func clearStaleDays(b *buckets, now time.Time) {
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location()).Unix()
	for i := 0; i < 31; i++ {
		if b.DayHit[i] > 0 && b.DayHit[i] < monthStart {
			b.DayUp[i] = 0
			b.DayDown[i] = 0
			b.DayHit[i] = 0
		}
	}
}

func clearStaleMonths(b *buckets, now time.Time) {
	yearStart := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location()).Unix()
	for i := 0; i < 12; i++ {
		if b.MonthHit[i] > 0 && b.MonthHit[i] < yearStart {
			b.MonthUp[i] = 0
			b.MonthDown[i] = 0
			b.MonthHit[i] = 0
		}
	}
}

func ensureLen(s *[]int64, n int) {
	if len(*s) == n {
		return
	}
	out := make([]int64, n)
	copy(out, *s)
	*s = out
}

func getSeries(mode string) (map[string]any, error) {
	b, err := loadBuckets()
	if err != nil {
		return nil, err
	}
	ensureLen(&b.HourUp, 24)
	ensureLen(&b.HourDown, 24)
	ensureLen(&b.WeekUp, weekSlots)
	ensureLen(&b.WeekDown, weekSlots)
	ensureLen(&b.DayUp, 31)
	ensureLen(&b.DayDown, 31)
	ensureLen(&b.MonthUp, 12)
	ensureLen(&b.MonthDown, 12)

	switch mode {
	case "", "day":
		labels := make([]string, 24)
		for i := 0; i < 24; i++ {
			labels[i] = fmt.Sprintf("%02d:00", i)
		}
		return map[string]any{"mode": "day", "up": b.HourUp, "down": b.HourDown, "labels": labels}, nil
	case "week", "day7":
		clearStaleWeek(b, time.Now())
		nowH := time.Now().Unix() / 3600
		labels := make([]string, weekSlots)
		up := make([]int64, weekSlots)
		down := make([]int64, weekSlots)
		for i := 0; i < weekSlots; i++ {
			abs := nowH - int64(weekSlots-1-i)
			slot := int(abs % weekSlots)
			if slot < 0 {
				slot += weekSlots
			}
			labels[i] = time.Unix(abs*3600, 0).Local().Format("01-02 15:00")
			up[i] = b.WeekUp[slot]
			down[i] = b.WeekDown[slot]
		}
		return map[string]any{"mode": "week", "up": up, "down": down, "labels": labels}, nil
	case "month":
		labels := make([]string, 31)
		for i := 0; i < 31; i++ {
			labels[i] = fmt.Sprintf("%d", i+1)
		}
		return map[string]any{"mode": "month", "up": b.DayUp, "down": b.DayDown, "labels": labels}, nil
	case "year":
		labels := []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
		return map[string]any{"mode": "year", "up": b.MonthUp, "down": b.MonthDown, "labels": labels}, nil
	default:
		return nil, fmt.Errorf("mode must be day|week|month|year")
	}
}

func getTrackers() (map[string]any, error) {
	trackers, err := loadTrackers()
	if err != nil {
		return nil, err
	}
	rows := sortedTrackerStats(trackers)
	return map[string]any{
		"trackers": rows,
		"count":    len(rows),
		"cap":      maxTrackerHosts,
		"note":     "interval sampling; host from announce URL; unknown when trackers.list unavailable",
	}, nil
}

func exportStats(format, scope string) (map[string]any, error) {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		format = "json"
	}
	scope = strings.ToLower(strings.TrimSpace(scope))
	if scope == "" {
		scope = "all"
	}
	if format != "json" && format != "csv" {
		return nil, fmt.Errorf("format must be json|csv")
	}
	if scope != "global" && scope != "trackers" && scope != "all" {
		return nil, fmt.Errorf("scope must be global|trackers|all")
	}

	day, _ := getSeries("day")
	week, _ := getSeries("week")
	month, _ := getSeries("month")
	year, _ := getSeries("year")
	tr, _ := getTrackers()

	payload := map[string]any{
		"exportedAt": time.Now().UTC().Format(time.RFC3339),
		"scope":      scope,
	}
	if scope == "global" || scope == "all" {
		payload["global"] = map[string]any{"day": day, "week": week, "month": month, "year": year}
	}
	if scope == "trackers" || scope == "all" {
		payload["trackers"] = tr["trackers"]
	}

	if format == "json" {
		raw, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"ok":       true,
			"format":   "json",
			"scope":    scope,
			"filename": "fleet-trafic.json",
			"content":  string(raw),
		}, nil
	}

	var b strings.Builder
	w := csv.NewWriter(&b)
	_ = w.Write([]string{"section", "key", "up", "down"})
	if scope == "global" || scope == "all" {
		writeSeriesCSV(w, "day", day)
		writeSeriesCSV(w, "month", month)
		writeSeriesCSV(w, "year", year)
	}
	if scope == "trackers" || scope == "all" {
		rows, _ := tr["trackers"].([]trackerStat)
		if rows == nil {
			if raw, ok := tr["trackers"]; ok {
				buf, _ := json.Marshal(raw)
				_ = json.Unmarshal(buf, &rows)
			}
		}
		for _, row := range rows {
			_ = w.Write([]string{"tracker", row.Host, strconv.FormatInt(row.Up, 10), strconv.FormatInt(row.Down, 10)})
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}
	return map[string]any{
		"ok":       true,
		"format":   "csv",
		"scope":    scope,
		"filename": "fleet-trafic.csv",
		"content":  b.String(),
	}, nil
}

func writeSeriesCSV(w *csv.Writer, name string, series map[string]any) {
	if series == nil {
		return
	}
	labels, _ := series["labels"].([]string)
	up := toInt64Slice(series["up"])
	down := toInt64Slice(series["down"])
	n := len(up)
	if len(down) < n {
		n = len(down)
	}
	for i := 0; i < n; i++ {
		key := strconv.Itoa(i)
		if i < len(labels) {
			key = labels[i]
		}
		_ = w.Write([]string{name, key, strconv.FormatInt(up[i], 10), strconv.FormatInt(down[i], 10)})
	}
}

func toInt64Slice(v any) []int64 {
	switch x := v.(type) {
	case []int64:
		return x
	case []any:
		out := make([]int64, 0, len(x))
		for _, e := range x {
			switch n := e.(type) {
			case float64:
				out = append(out, int64(n))
			case int64:
				out = append(out, n)
			case json.Number:
				i, _ := n.Int64()
				out = append(out, i)
			}
		}
		return out
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		var out []int64
		_ = json.Unmarshal(raw, &out)
		return out
	}
}

func loadSettings() (settings, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "settings"})
	if err != nil {
		return settings{}, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return defaultSettings(), nil
	}
	var st settings
	if err := json.Unmarshal(raw, &st); err != nil {
		return defaultSettings(), nil
	}
	if st.IntervalMin == 0 {
		st.IntervalMin = 15
	}
	return st, nil
}

func saveSettings(st *settings) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "settings", "value": st})
	return err
}

func loadBuckets() (*buckets, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "buckets"})
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return emptyBuckets(), nil
	}
	var b buckets
	if err := json.Unmarshal(raw, &b); err != nil {
		return emptyBuckets(), nil
	}
	return &b, nil
}

func saveBuckets(b *buckets) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "buckets", "value": b})
	return err
}

func loadTrackers() (map[string]*trackerStat, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "trackers"})
	if err != nil {
		return nil, err
	}
	out := map[string]*trackerStat{}
	if string(raw) == "null" || len(raw) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]*trackerStat{}, nil
	}
	if out == nil {
		out = map[string]*trackerStat{}
	}
	return out, nil
}

func saveTrackers(m map[string]*trackerStat) error {
	if m == nil {
		m = map[string]*trackerStat{}
	}
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "trackers", "value": m})
	return err
}

func loadHashState() (map[string]hashState, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "hashState"})
	if err != nil {
		return nil, err
	}
	out := map[string]hashState{}
	if string(raw) == "null" || len(raw) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]hashState{}, nil
	}
	if out == nil {
		out = map[string]hashState{}
	}
	return out, nil
}

func saveHashState(m map[string]hashState) error {
	if m == nil {
		m = map[string]hashState{}
	}
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "hashState", "value": m})
	return err
}

func hostCall(method string, params any) (json.RawMessage, error) {
	ch := make(chan hostReply, 1)
	mu.Lock()
	id := nextHost
	nextHost++
	pending[id] = ch
	mu.Unlock()

	b, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      id,
	})
	if err != nil {
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, err
	}
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()

	select {
	case reply := <-ch:
		if reply.err != "" {
			return nil, fmt.Errorf("%s", reply.err)
		}
		if len(reply.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return reply.result, nil
	case <-time.After(30 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "result": result, "id": id})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func replyErr(id any, msg string) {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"error":   map[string]any{"code": -32000, "message": msg},
		"id":      id,
	})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
