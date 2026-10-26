package fencing

import "testing"

func TestSourceRelayStageTicketValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		ticket string
		valid  bool
	}{
		{name: "lowercase sha256", ticket: "0123456789abcdef" + "0123456789abcdef" + "0123456789abcdef" + "0123456789abcdef", valid: true},
		{name: "uppercase is not canonical", ticket: "0123456789abcdef" + "0123456789abcdef" + "0123456789abcdef" + "0123456789ABCDEf"},
		{name: "wrong length", ticket: "0123456789abcdef"},
		{name: "non-hex", ticket: "0123456789abcdef" + "0123456789abcdef" + "0123456789abcdef" + "0123456789abcdeg"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validStageTicket(test.ticket); got != test.valid {
				t.Fatalf("validStageTicket(%q) = %t, want %t", test.ticket, got, test.valid)
			}
		})
	}
}
