package otelpgx

import "testing"

func TestSqlOperationName(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		expName string
	}{
		{
			name:    "Spaces only",
			query:   "SELECT * FROM users",
			expName: "SELECT",
		},
		{
			name:    "Newline and tab",
			query:   "UPDATE\n\tfoo",
			expName: "UPDATE",
		},
		{
			name:    "Additional whitespace",
			query:   " \n SELECT\n\t   *   FROM users  ",
			expName: "SELECT",
		},
		{
			name:    "Whitespace-only query",
			query:   " \n\t",
			expName: "UNKNOWN",
		},
		{
			name:    "Empty query",
			query:   "",
			expName: "UNKNOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := sqlOperationName(tt.query)
			if name != tt.expName {
				t.Errorf("Got name %q, expected %q", name, tt.expName)
			}
		})
	}
}
