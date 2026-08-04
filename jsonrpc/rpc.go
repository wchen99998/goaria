package jsonrpc

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/wchen99998/goaria"
)

const (
	rpcParseError     = -32700
	rpcInvalidRequest = -32600
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
	rpcInternalError  = -32603
	rpcAriaError      = 1
	maxBatchCalls     = 64
	maxMulticallCalls = 64
	maxRPCIDStringLen = 64
	maxSafeIntegerID  = int64(1<<53 - 1)
)

var rpcMethods = []string{
	"aria2.addUri",
	"aria2.addTorrent",
	"aria2.remove",
	"aria2.forceRemove",
	"aria2.pause",
	"aria2.pauseAll",
	"aria2.forcePause",
	"aria2.forcePauseAll",
	"aria2.unpause",
	"aria2.unpauseAll",
	"aria2.tellStatus",
	"aria2.getUris",
	"aria2.getFiles",
	"aria2.getPeers",
	"aria2.getServers",
	"aria2.tellActive",
	"aria2.tellWaiting",
	"aria2.tellStopped",
	"aria2.changePosition",
	"aria2.changeUri",
	"aria2.getOption",
	"aria2.changeOption",
	"aria2.getGlobalOption",
	"aria2.changeGlobalOption",
	"aria2.getGlobalStat",
	"aria2.purgeDownloadResult",
	"aria2.removeDownloadResult",
	"aria2.getVersion",
	"aria2.getSessionInfo",
	"aria2.shutdown",
	"aria2.forceShutdown",
	"aria2.saveSession",
	"system.multicall",
	"system.listMethods",
	"system.listNotifications",
}

type RPCHandler struct {
	engine *goaria.Engine
	secret string
}

type Handler = RPCHandler

type rpcCall struct {
	JSONRPC string
	Method  string
	Params  []json.RawMessage
	ID      *json.RawMessage
	HasID   bool
}

type rpcResponse struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id"`
	Result  any              `json:"result,omitempty"`
	Error   *rpcErrorBody    `json:"error,omitempty"`
}

func (r rpcResponse) MarshalJSON() ([]byte, error) {
	if r.Error != nil {
		return json.Marshal(struct {
			JSONRPC string           `json:"jsonrpc"`
			ID      *json.RawMessage `json:"id"`
			Error   *rpcErrorBody    `json:"error"`
		}{r.JSONRPC, r.ID, r.Error})
	}
	return json.Marshal(struct {
		JSONRPC string           `json:"jsonrpc"`
		ID      *json.RawMessage `json:"id"`
		Result  any              `json:"result"`
	}{r.JSONRPC, r.ID, r.Result})
}

type rpcErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type callError struct {
	code int
	msg  string
}

func (e callError) Error() string {
	return e.msg
}

func NewRPCHandler(engine *goaria.Engine, secret string) *RPCHandler {
	return &RPCHandler{engine: engine, secret: secret}
}

func NewHandler(engine *goaria.Engine, secret string) *Handler {
	return NewRPCHandler(engine, secret)
}

func (h *RPCHandler) payloadProvesSecret(payload []byte) bool {
	if h.secret == "" {
		return true
	}
	payload = bytes.TrimSpace(payload)
	var calls []json.RawMessage
	if len(payload) > 0 && payload[0] == '[' {
		var err error
		calls, err = decodeRawArrayLimited(payload, maxBatchCalls)
		if err != nil {
			return false
		}
	} else {
		calls = []json.RawMessage{payload}
	}
	for _, raw := range calls {
		call, err := parseCall(raw)
		if err != nil || call.JSONRPC != "2.0" || call.Method == "" {
			continue
		}
		if call.Method == "system.multicall" {
			if h.multicallProvesSecret(call.Params) {
				return true
			}
			continue
		}
		if methodNeedsSecret(call.Method) && len(call.Params) > 0 && h.matchesSecretParam(call.Params[0]) {
			return true
		}
	}
	return false
}

func methodNeedsSecret(method string) bool {
	return method != "system.listMethods" && method != "system.listNotifications" && method != "system.multicall"
}

func (h *RPCHandler) matchesSecretParam(raw json.RawMessage) bool {
	var token string
	if err := decodeUseNumber(raw, &token); err != nil {
		return false
	}
	want := "token:" + h.secret
	return len(token) == len(want) && subtle.ConstantTimeCompare([]byte(token), []byte(want)) == 1
}

func (h *RPCHandler) multicallProvesSecret(params []json.RawMessage) bool {
	if len(params) != 1 {
		return false
	}
	calls, err := decodeRawArrayLimited(params[0], maxMulticallCalls)
	if err != nil {
		return false
	}
	for _, raw := range calls {
		obj, err := decodeRawObjectLimited(raw, 4)
		if err != nil {
			continue
		}
		var method string
		if err := decodeUseNumber(obj["methodName"], &method); err != nil || !methodNeedsSecret(method) {
			continue
		}
		callParams, err := decodeRawArrayLimited(obj["params"], maxRPCParams)
		if err == nil && len(callParams) > 0 && h.matchesSecretParam(callParams[0]) {
			return true
		}
	}
	return false
}

func (h *RPCHandler) HandlePayload(payload []byte) ([]byte, bool) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return mustJSON(errorResponse(nil, rpcParseError, "Parse error")), true
	}
	if payload[0] == '[' {
		raws, err := decodeRawArrayLimited(payload, maxBatchCalls)
		if err != nil {
			if errors.Is(err, errJSONCollectionTooLarge) {
				return mustJSON(errorResponse(nil, rpcInvalidRequest, "Invalid Request")), true
			}
			return mustJSON(errorResponse(nil, rpcParseError, "Parse error")), true
		}
		if len(raws) == 0 {
			return mustJSON(errorResponse(nil, rpcInvalidRequest, "Invalid Request")), true
		}
		responses := make([]rpcResponse, 0, len(raws))
		for _, raw := range raws {
			if resp, ok := h.handleOne(raw); ok {
				responses = append(responses, resp)
			}
		}
		if len(responses) == 0 {
			return nil, false
		}
		return mustJSON(responses), true
	}
	resp, ok := h.handleOne(payload)
	if !ok {
		return nil, false
	}
	return mustJSON(resp), true
}

func (h *RPCHandler) handleOne(raw []byte) (rpcResponse, bool) {
	call, err := parseCall(raw)
	if err != nil {
		var ce callError
		if errors.As(err, &ce) {
			// A structurally valid JSON-RPC notification never receives a
			// response, including when its params member has the wrong shape.
			if ce.code == rpcInvalidParams && !call.HasID && call.JSONRPC == "2.0" && call.Method != "" {
				return rpcResponse{}, false
			}
			return errorResponse(call.ID, ce.code, ce.msg), true
		}
		if isJSONParseError(err) {
			return errorResponse(nil, rpcParseError, "Parse error"), true
		}
		return errorResponse(nil, rpcInvalidRequest, "Invalid Request"), true
	}
	if call.Method == "" || call.JSONRPC != "2.0" {
		return errorResponse(call.ID, rpcInvalidRequest, "Invalid Request"), true
	}
	params, err := h.stripSecret(call.Method, call.Params)
	if err != nil {
		if !call.HasID {
			return rpcResponse{}, false
		}
		return errorResponse(call.ID, rpcAriaError, err.Error()), true
	}
	result, err := h.invoke(call.Method, params)
	if !call.HasID {
		return rpcResponse{}, false
	}
	if err != nil {
		return errorResponse(call.ID, rpcCode(err), err.Error()), true
	}
	return rpcResponse{JSONRPC: "2.0", ID: call.ID, Result: result}, true
}

func parseCall(raw []byte) (rpcCall, error) {
	obj, err := decodeRawObjectLimited(raw, maxRPCObjectFields)
	if err != nil {
		return rpcCall{}, err
	}
	var call rpcCall
	if v, ok := obj["jsonrpc"]; ok {
		_ = decodeUseNumber(v, &call.JSONRPC)
	}
	if v, ok := obj["method"]; ok {
		_ = decodeUseNumber(v, &call.Method)
	}
	if v, ok := obj["id"]; ok {
		if !validRPCID(v) {
			return rpcCall{}, callError{code: rpcInvalidRequest, msg: "Invalid Request"}
		}
		idCopy := append(json.RawMessage(nil), v...)
		call.ID = &idCopy
		call.HasID = true
	}
	if v, ok := obj["params"]; ok {
		trimmed := bytes.TrimSpace(v)
		if len(trimmed) > 0 && trimmed[0] == '[' {
			call.Params, err = decodeRawArrayLimited(trimmed, maxRPCParams)
			if err != nil {
				return call, callError{code: rpcInvalidParams, msg: "Invalid params"}
			}
		} else {
			return call, callError{code: rpcInvalidParams, msg: "Invalid params"}
		}
	}
	return call, nil
}

func validRPCID(raw json.RawMessage) bool {
	var value any
	if err := decodeUseNumber(raw, &value); err != nil {
		return false
	}
	switch value := value.(type) {
	case nil:
		return true
	case string:
		return len(value) <= maxRPCIDStringLen
	case json.Number:
		n, err := strconv.ParseInt(value.String(), 10, 64)
		return err == nil && n >= -maxSafeIntegerID && n <= maxSafeIntegerID
	default:
		return false
	}
}

func (h *RPCHandler) stripSecret(method string, params []json.RawMessage) ([]json.RawMessage, error) {
	if method == "system.listMethods" || method == "system.listNotifications" || method == "system.multicall" {
		return params, nil
	}
	if len(params) == 0 {
		if h.secret != "" {
			return nil, fmt.Errorf("Unauthorized")
		}
		return params, nil
	}
	var token string
	if err := decodeUseNumber(params[0], &token); err == nil && strings.HasPrefix(token, "token:") {
		got := strings.TrimPrefix(token, "token:")
		if h.secret != "" && subtle.ConstantTimeCompare([]byte(got), []byte(h.secret)) != 1 {
			return nil, fmt.Errorf("Unauthorized")
		}
		return params[1:], nil
	}
	if h.secret != "" {
		return nil, fmt.Errorf("Unauthorized")
	}
	return params, nil
}

func errorResponse(id *json.RawMessage, code int, msg string) rpcResponse {
	return rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &rpcErrorBody{Code: code, Message: msg},
	}
}

func rpcCode(err error) int {
	var ce callError
	if errors.As(err, &ce) {
		return ce.code
	}
	if errors.Is(err, goaria.ErrInvalidParams) {
		return rpcInvalidParams
	}
	return rpcAriaError
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		b, _ = json.Marshal(errorResponse(nil, rpcInternalError, err.Error()))
	}
	return b
}
