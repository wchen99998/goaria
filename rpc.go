package goaria

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	rpcParseError     = -32700
	rpcInvalidRequest = -32600
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
	rpcInternalError  = -32603
	rpcAriaError      = 1
)

var rpcMethods = []string{
	"aria2.addUri",
	"aria2.addTorrent",
	"aria2.addMetalink",
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
	engine *Engine
	secret string
}

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

func NewRPCHandler(engine *Engine, secret string) *RPCHandler {
	return &RPCHandler{engine: engine, secret: secret}
}

func (h *RPCHandler) HandlePayload(payload []byte) ([]byte, bool) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return mustJSON(errorResponse(nil, rpcInvalidRequest, "Invalid Request")), true
	}
	if payload[0] == '[' {
		var raws []json.RawMessage
		if err := decodeUseNumber(payload, &raws); err != nil {
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
			return errorResponse(call.ID, ce.code, ce.msg), true
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
	var obj map[string]json.RawMessage
	if err := decodeUseNumber(raw, &obj); err != nil {
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
		idCopy := append(json.RawMessage(nil), v...)
		call.ID = &idCopy
		call.HasID = true
	}
	if v, ok := obj["params"]; ok {
		if len(v) > 0 && v[0] == '[' {
			if err := decodeUseNumber(v, &call.Params); err != nil {
				return call, callError{code: rpcInvalidParams, msg: "Invalid params"}
			}
		} else {
			return call, callError{code: rpcInvalidParams, msg: "Invalid params"}
		}
	}
	return call, nil
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
	if errors.Is(err, ErrInvalidParams) {
		return rpcInvalidParams
	}
	if errors.Is(err, ErrUnsupportedMethod) || errors.Is(err, ErrUnsupportedProtocol) || errors.Is(err, ErrNotFound) || errors.Is(err, ErrInvalidGID) {
		return rpcAriaError
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
