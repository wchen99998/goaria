package jsonrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/wchen99998/goaria"
)

var errTrailingJSONData = errors.New("invalid trailing JSON data")
var errJSONCollectionTooLarge = errors.New("JSON collection too large")

const (
	maxStringListItems = 64
	maxOptionCount     = 128
	maxRPCParams       = 8
	maxRPCObjectFields = 16
)

func stringParam(params []json.RawMessage, i int) (string, error) {
	if i >= len(params) {
		return "", callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	trimmed := bytes.TrimSpace(params[i])
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	var s string
	if err := decodeUseNumber(trimmed, &s); err != nil {
		return "", callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	return s, nil
}

func stringSliceParam(params []json.RawMessage, i int) ([]string, error) {
	if i >= len(params) {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	trimmed := bytes.TrimSpace(params[i])
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	rawItems, err := decodeRawArrayLimited(trimmed, maxStringListItems)
	if err != nil {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	out := make([]string, 0, len(rawItems))
	for _, raw := range rawItems {
		var item string
		if err := decodeUseNumber(raw, &item); err != nil {
			return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
		}
		out = append(out, item)
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
	trimmed := bytes.TrimSpace(params[i])
	if len(trimmed) == 0 || (trimmed[0] != '-' && (trimmed[0] < '0' || trimmed[0] > '9')) {
		return 0, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	var num json.Number
	if err := decodeUseNumber(trimmed, &num); err != nil {
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
	trimmed := bytes.TrimSpace(params[i])
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	rawValues, err := decodeRawObjectLimited(trimmed, maxOptionCount)
	if err != nil || rawValues == nil {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	raw := make(map[string]any, len(rawValues))
	for key, encoded := range rawValues {
		encoded = bytes.TrimSpace(encoded)
		if len(encoded) > 0 && encoded[0] == '[' {
			items, err := decodeRawArrayLimited(encoded, maxStringListItems)
			if err != nil {
				return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
			}
			values := make([]any, 0, len(items))
			for _, item := range items {
				var s string
				if err := decodeUseNumber(item, &s); err != nil {
					return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
				}
				values = append(values, s)
			}
			raw[key] = values
			continue
		}
		var value any
		if err := decodeUseNumber(encoded, &value); err != nil {
			return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
		}
		switch value := value.(type) {
		case nil, string, json.Number, bool:
			raw[key] = value
		default:
			return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
		}
	}
	return goaria.Options(raw), nil
}

// decodeRawArrayLimited parses a JSON array without first materializing every
// element. This matters for inputs such as [0,0,...], where a small wire payload
// can otherwise allocate a very large slice of interfaces or RawMessages.
func decodeRawArrayLimited(data []byte, maxItems int) ([]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '[' {
		return nil, fmt.Errorf("expected JSON array")
	}
	items := make([]json.RawMessage, 0, min(maxItems, 8))
	for dec.More() {
		if len(items) >= maxItems {
			return nil, errJSONCollectionTooLarge
		}
		var item json.RawMessage
		if err := dec.Decode(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	if err := requireJSONEOF(dec); err != nil {
		return nil, err
	}
	return items, nil
}

func decodeRawObjectLimited(data []byte, maxFields int) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return nil, fmt.Errorf("expected JSON object")
	}
	values := make(map[string]json.RawMessage, min(maxFields, 8))
	fields := 0
	for dec.More() {
		if fields >= maxFields {
			return nil, errJSONCollectionTooLarge
		}
		keyToken, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("expected JSON object key")
		}
		if _, duplicate := values[key]; duplicate {
			return nil, fmt.Errorf("duplicate JSON object key %q", key)
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		values[key] = value
		fields++
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	if err := requireJSONEOF(dec); err != nil {
		return nil, err
	}
	return values, nil
}

func requireJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errTrailingJSONData
		}
		return err
	}
	return nil
}

func decodeUseNumber(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return requireJSONEOF(dec)
}

func isJSONParseError(err error) bool {
	var syntaxErr *json.SyntaxError
	return errors.As(err, &syntaxErr) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, errTrailingJSONData)
}
