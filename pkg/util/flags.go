package util

import (
	"flag"
	"net/url"
	"time"
)

const millisecondsInDay = 24 * 60 * 60 * 1000

// Registerer is a thing that can RegisterFlags
type Registerer interface {
	RegisterFlags(*flag.FlagSet)
}

// RegisterFlags registers flags with the provided Registerers
func RegisterFlags(rs ...Registerer) {
	for _, r := range rs {
		r.RegisterFlags(flag.CommandLine)
	}
}

// DayValue is a time value that can be used as a flag.
// NB it only parses days!
type DayValue struct {
	Time int64 // milliseconds since the epoch
	set  bool
}

// NewDayValue makes a new DayValue; will round t down to the nearest midnight.
func NewDayValue(t int64) DayValue {
	return DayValue{
		Time: (t / millisecondsInDay) * millisecondsInDay,
		set:  true,
	}
}

func (v DayValue) Unix() int64 {
	return v.Time / 1000
}

// String implements flag.Value
func (v DayValue) String() string {
	return time.Unix(v.Unix(), 0).Format(time.RFC3339)
}

// Set implements flag.Value
func (v *DayValue) Set(s string) error {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return err
	}
	v.Time = t.Unix() * 1000
	v.set = true
	return nil
}

// IsSet returns true is the DayValue has been set.
func (v *DayValue) IsSet() bool {
	return v.set
}

// URLValue is a url.URL that can be used as a flag.
type URLValue struct {
	*url.URL
}

// String implements flag.Value
func (v URLValue) String() string {
	if v.URL == nil {
		return ""
	}
	return v.URL.String()
}

// Set implements flag.Value
func (v *URLValue) Set(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	v.URL = u
	return nil
}
