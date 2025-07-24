package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleTableInsertMarkers(t *testing.T) {
	testCases := []struct {
		Count          int
		ExpectedOutput string
	}{
		{
			Count:          0,
			ExpectedOutput: "",
		},
		{
			Count:          1,
			ExpectedOutput: "($1,$2,$3,$4,pg_current_xact_id())",
		},
		{
			Count:          2,
			ExpectedOutput: "($1,$2,$3,$4,pg_current_xact_id()),($5,$6,$7,$8,pg_current_xact_id())",
		},
		{
			Count: 5,
			ExpectedOutput: "($1,$2,$3,$4,pg_current_xact_id())," +
				"($5,$6,$7,$8,pg_current_xact_id())," +
				"($9,$10,$11,$12,pg_current_xact_id())," +
				"($13,$14,$15,$16,pg_current_xact_id())," +
				"($17,$18,$19,$20,pg_current_xact_id())",
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d", tc.Count), func(t *testing.T) {
			output := singleTableInsertMarkers(tc.Count)
			assert.Equal(t, tc.ExpectedOutput, output)
		})
	}
}
