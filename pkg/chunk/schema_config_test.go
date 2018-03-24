package chunk

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/test"
)

func TestHourlyBuckets(t *testing.T) {
	const (
		userID     = "0"
		metricName = model.LabelValue("name")
		tableName  = "table"
	)
	var cfg = SchemaConfig{
		OriginalTableName: tableName,
	}

	type args struct {
		from    int64
		through int64
	}
	tests := []struct {
		name string
		args args
		want []Bucket
	}{
		{
			"0 hour window",
			args{
				from:    0,
				through: 0,
			},
			[]Bucket{},
		},
		{
			"30 minute window",
			args{
				from:    0,
				through: 1800 * 1000,
			},
			[]Bucket{{
				from:      0,
				through:   1800 * 1000, // ms
				tableName: "table",
				hashKey:   "0:0",
			}},
		},
		{
			"1 hour window",
			args{
				from:    0,
				through: 3600 * 1000,
			},
			[]Bucket{{
				from:      0,
				through:   3600 * 1000, // ms
				tableName: "table",
				hashKey:   "0:0",
			}},
		},
		{
			"window spanning 3 hours with non-zero start",
			args{
				from:    900 * 1000,
				through: ((2 * 3600) + 1800) * 1000,
			},
			[]Bucket{{
				from:      900 * 1000,  // ms
				through:   3600 * 1000, // ms
				tableName: "table",
				hashKey:   "0:0",
			}, {
				from:      0,
				through:   3600 * 1000, // ms
				tableName: "table",
				hashKey:   "0:1",
			}, {
				from:      0,
				through:   1800 * 1000, // ms
				tableName: "table",
				hashKey:   "0:2",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cfg.hourlyBuckets(tt.args.from, tt.args.through, userID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SchemaConfig.dailyBuckets() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDailyBuckets(t *testing.T) {
	const (
		userID     = "0"
		metricName = model.LabelValue("name")
		tableName  = "table"
	)
	var cfg = SchemaConfig{
		OriginalTableName: tableName,
	}

	type args struct {
		from    int64
		through int64
	}
	tests := []struct {
		name string
		args args
		want []Bucket
	}{
		{
			"0 day window",
			args{
				from:    0,
				through: 0,
			},
			[]Bucket{},
		},
		{
			"6 hour window",
			args{
				from:    0,
				through: (6 * 3600) * 1000,
			},
			[]Bucket{{
				from:      0,
				through:   (6 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d0",
			}},
		},
		{
			"1 day window",
			args{
				from:    0,
				through: (24 * 3600) * 1000,
			},
			[]Bucket{{
				from:      0,
				through:   (24 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d0",
			}},
		},
		{
			"window spanning 3 days with non-zero start",
			args{
				from:    (6 * 3600) * 1000,
				through: ((2 * 24 * 3600) + (12 * 3600)) * 1000,
			},
			[]Bucket{{
				from:      (6 * 3600) * 1000,  // ms
				through:   (24 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d0",
			}, {
				from:      0,
				through:   (24 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d1",
			}, {
				from:      0,
				through:   (12 * 3600) * 1000, // ms
				tableName: "table",
				hashKey:   "0:d2",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cfg.dailyBuckets(tt.args.from, tt.args.through, userID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SchemaConfig.dailyBuckets() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompositeSchema(t *testing.T) {
	type result struct {
		from, through int64
		schema        Schema
	}
	collect := func(results *[]result) func(from, through int64, schema Schema) ([]IndexEntry, error) {
		return func(from, through int64, schema Schema) ([]IndexEntry, error) {
			*results = append(*results, result{from, through, schema})
			return nil, nil
		}
	}
	cs := compositeSchema{
		schemas: []compositeSchemaEntry{
			{0, mockSchema(1)},
			{100 * 1000, mockSchema(2)},
			{200 * 1000, mockSchema(3)},
		},
	}

	for i, tc := range []struct {
		cs            compositeSchema
		from, through int64
		want          []result
	}{
		// Test we have sensible results when there are no schema's defined
		{compositeSchema{}, 0, 1, []result{}},

		// Test we have sensible results when there is a single schema
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{0, mockSchema(1)},
				},
			},
			0, 10,
			[]result{
				{0, 10 * 1000, mockSchema(1)},
			},
		},

		// Test we have sensible results for negative (ie pre 1970) times
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{0, mockSchema(1)},
				},
			},
			-10, -9,
			[]result{},
		},
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{0, mockSchema(1)},
				},
			},
			-10, 10,
			[]result{
				{0, 10 * 1000, mockSchema(1)},
			},
		},

		// Test we have sensible results when there is two schemas
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{0, mockSchema(1)},
					{100 * 1000, mockSchema(2)},
				},
			},
			34, 165,
			[]result{
				{34 * 1000, 100*1000 - 1, mockSchema(1)},
				{100 * 1000, 165 * 1000, mockSchema(2)},
			},
		},

		// Test we get only one result when two schema start at same time
		{
			compositeSchema{
				schemas: []compositeSchemaEntry{
					{0, mockSchema(1)},
					{10 * 1000, mockSchema(2)},
					{10 * 1000, mockSchema(3)},
				},
			},
			0, 165,
			[]result{
				{0, 10*1000 - 1, mockSchema(1)},
				{10 * 1000, 165 * 1000, mockSchema(3)},
			},
		},

		// Test all the various combination we can get when there are three schemas
		{
			cs, 34, 65,
			[]result{
				{34 * 1000, 65 * 1000, mockSchema(1)},
			},
		},

		{
			cs, 244, 6785,
			[]result{
				{244 * 1000, 6785 * 1000, mockSchema(3)},
			},
		},

		{
			cs, 34, 165,
			[]result{
				{34 * 1000, 100*1000 - 1, mockSchema(1)},
				{100 * 1000, 165 * 1000, mockSchema(2)},
			},
		},

		{
			cs, 151, 264,
			[]result{
				{151 * 1000, 200*1000 - 1, mockSchema(2)},
				{200 * 1000, 264 * 1000, mockSchema(3)},
			},
		},

		{
			cs, 32, 264,
			[]result{
				{32 * 1000, 100*1000 - 1, mockSchema(1)},
				{100 * 1000, 200*1000 - 1, mockSchema(2)},
				{200 * 1000, 264 * 1000, mockSchema(3)},
			},
		},
	} {
		t.Run(fmt.Sprintf("TestSchemaComposite[%d]", i), func(t *testing.T) {
			have := []result{}
			tc.cs.forSchemasIndexEntry(tc.from*1000, tc.through*1000, collect(&have))
			if !reflect.DeepEqual(tc.want, have) {
				t.Fatalf("wrong schemas - %s", test.Diff(tc.want, have))
			}
		})
	}
}
