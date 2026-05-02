package jsonrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/wchen99998/goaria"
)

func stringParam(params []json.RawMessage, i int) (string, error) {
	if i >= len(params) {
		return "", callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	var s string
	if err := decodeUseNumber(params[i], &s); err != nil {
		return "", callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	return s, nil
}

func stringSliceParam(params []json.RawMessage, i int) ([]string, error) {
	if i >= len(params) {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	var out []string
	if err := decodeUseNumber(params[i], &out); err != nil {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	return out, nil
}

func optionalStringSliceParam(params []json.RawMessage, i int) ([]string, error) {
	if i >= len(params) {
		return nil, nil
	}
	return stringSliceParam(params, i)
}

func intParam(params []json.RawMessage, i int) (int, error) {
	if i >= len(params) {
		return 0, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	var n int
	if err := decodeUseNumber(params[i], &n); err == nil {
		return n, nil
	}
	var num json.Number
	if err := decodeUseNumber(params[i], &num); err != nil {
		return 0, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	v, err := strconv.Atoi(num.String())
	if err != nil {
		return 0, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	return v, nil
}

func optionalIntParam(params []json.RawMessage, i int) (*int, error) {
	if i >= len(params) {
		return nil, nil
	}
	n, err := intParam(params, i)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func optionsParam(params []json.RawMessage, i int, required bool) (goaria.Options, error) {
	if i >= len(params) {
		if required {
			return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
		}
		return goaria.Options{}, nil
	}
	var raw map[string]any
	if err := decodeUseNumber(params[i], &raw); err != nil {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	return goaria.Options(raw), nil
}

func decodeUseNumber(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("invalid trailing JSON data")
		}
		return err
	}
	return nil
}
