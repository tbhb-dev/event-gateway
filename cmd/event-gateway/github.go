// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import "encoding/json"

//nolint:tagliatelle // The envelope uses the gateway protocol field names.
type envelope struct {
	Type       string  `json:"type"`
	Provider   string  `json:"provider"`
	Event      string  `json:"event"`
	Delivery   string  `json:"delivery"`
	Repository *string `json:"repository"`
	ReceivedAt string  `json:"received_at"`
	Payload    []byte  `json:"-"`
}

func (event envelope) encode() []byte {
	//nolint:errcheck,errchkjson // Envelope metadata contains only strings and null.
	data, _ := json.Marshal(event)
	// The payload is already JSON. Appending it preserves the signed bytes.
	data = append(data[:len(data)-1], `,"payload":`...)
	data = append(data, event.Payload...)

	return append(data, '}')
}
