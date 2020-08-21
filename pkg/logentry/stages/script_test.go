package stages

import (
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
	"testing"
	"time"
)

func TestScriptStage(t *testing.T) {

	emptyTime := time.Time{}

	tests := map[string]struct {
		config            string
		loglines          []string
		expectedExtracted map[string]interface{}
		expectedLabels    map[string]string
		expectedEntry     string
		expectedTime      time.Time
	}{
		"condition": {
			`
pipeline_stages:
  - regex:
      expression: items:(?P<items>\S+)
  - script:
      text: 
        if int(source["items"],0) > 1 { 

          label["type"]="multi_item" 

        } else { 

           label["type"]="item" 

        }
`, []string{"items:2"},

			map[string]interface{}{
				"items": "2",
			},
			map[string]string{
				"type": "multi_item",
			},
			"",
			emptyTime,
		},
		"state": {
			`
pipeline_stages:
  - regex:
      expression: value:(?P<value>\S+)
  - script:
      state:
         count_a: 0
      text:
        if source["value"]=="a" { 

          count_a++ 

        }

        if count_a == 2 { 

           count_a = 0

           label["2as"] = "true"

        }
`, []string{"value:a",
				"value:b",
				"value:a",
				"value:c"},
			nil,
			map[string]string{
				"2as": "true",
			},
			"",
			emptyTime,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var config map[string]interface{}

			err := yaml.Unmarshal([]byte(test.config), &config)
			if err != nil {
				t.Fatal(err)
			}

			st, err := NewPipeline(util.Logger, config["pipeline_stages"].([]interface{}), nil, prometheus.DefaultRegisterer)
			if err != nil {
				t.Fatal(err)
			}

			var lbls model.LabelSet
			var extracted map[string]interface{}
			var timestamp time.Time
			var entry string
			for _, s := range test.loglines {
				lbls = model.LabelSet{}
				extracted = map[string]interface{}{}
				timestamp = time.Now()
				entry = s
				st.Process(lbls, extracted, &timestamp, &entry)
			}
			if test.expectedExtracted != nil {
				assert.Equal(t, test.expectedExtracted, extracted)
			}
			if test.expectedLabels != nil {
				for k, v := range test.expectedLabels {
					assert.Equal(t, test.expectedLabels[k], v)
				}
			}
			if test.expectedEntry != "" {
				assert.Equal(t, test.expectedEntry, entry)
			}
			emptyTime := time.Time{}
			if test.expectedTime != emptyTime {
				assert.Equal(t, test.expectedTime, timestamp)
			}
		})
	}
}
