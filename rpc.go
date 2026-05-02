package goaria

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
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

func (h *RPCHandler) invoke(method string, params []json.RawMessage) (any, error) {
	switch method {
	case "aria2.addUri":
		uris, err := stringSliceParam(params, 0)
		if err != nil {
			return nil, err
		}
		opts, err := optionsParam(params, 1, false)
		if err != nil {
			return nil, err
		}
		pos, err := optionalIntParam(params, 2)
		if err != nil {
			return nil, err
		}
		return h.engine.AddURI(uris, opts, pos)
	case "aria2.addTorrent":
		torrent, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		uris, _ := optionalStringSliceParam(params, 1)
		opts, err := optionsParam(params, 2, false)
		if err != nil {
			return nil, err
		}
		pos, err := optionalIntParam(params, 3)
		if err != nil {
			return nil, err
		}
		return h.engine.AddTorrent(torrent, uris, opts, pos)
	case "aria2.addMetalink":
		metalink, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		opts, err := optionsParam(params, 1, false)
		if err != nil {
			return nil, err
		}
		pos, err := optionalIntParam(params, 2)
		if err != nil {
			return nil, err
		}
		return h.engine.AddMetalink(metalink, opts, pos)
	case "aria2.remove":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.Remove(gid)
	case "aria2.forceRemove":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.ForceRemove(gid)
	case "aria2.pause":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.Pause(gid)
	case "aria2.pauseAll":
		return h.engine.PauseAll()
	case "aria2.forcePause":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.ForcePause(gid)
	case "aria2.forcePauseAll":
		return h.engine.ForcePauseAll()
	case "aria2.unpause":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.Unpause(gid)
	case "aria2.unpauseAll":
		return h.engine.UnpauseAll()
	case "aria2.tellStatus":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		keys, err := optionalStringSliceParam(params, 1)
		if err != nil {
			return nil, err
		}
		return h.engine.TellStatus(gid, keys)
	case "aria2.getUris":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.GetURIs(gid)
	case "aria2.getFiles":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.GetFiles(gid)
	case "aria2.getPeers":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.GetPeers(gid)
	case "aria2.getServers":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.GetServers(gid)
	case "aria2.tellActive":
		keys, err := optionalStringSliceParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.TellActive(keys), nil
	case "aria2.tellWaiting":
		offset, err := intParam(params, 0)
		if err != nil {
			return nil, err
		}
		num, err := intParam(params, 1)
		if err != nil {
			return nil, err
		}
		keys, err := optionalStringSliceParam(params, 2)
		if err != nil {
			return nil, err
		}
		return h.engine.TellWaiting(offset, num, keys), nil
	case "aria2.tellStopped":
		offset, err := intParam(params, 0)
		if err != nil {
			return nil, err
		}
		num, err := intParam(params, 1)
		if err != nil {
			return nil, err
		}
		keys, err := optionalStringSliceParam(params, 2)
		if err != nil {
			return nil, err
		}
		return h.engine.TellStopped(offset, num, keys), nil
	case "aria2.changePosition":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		pos, err := intParam(params, 1)
		if err != nil {
			return nil, err
		}
		how, err := stringParam(params, 2)
		if err != nil {
			return nil, err
		}
		return h.engine.ChangePosition(gid, pos, how)
	case "aria2.changeUri":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		fileIndex, err := intParam(params, 1)
		if err != nil {
			return nil, err
		}
		del, err := stringSliceParam(params, 2)
		if err != nil {
			return nil, err
		}
		add, err := stringSliceParam(params, 3)
		if err != nil {
			return nil, err
		}
		pos, err := optionalIntParam(params, 4)
		if err != nil {
			return nil, err
		}
		return h.engine.ChangeURI(gid, fileIndex, del, add, pos)
	case "aria2.getOption":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.GetOption(gid)
	case "aria2.changeOption":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		opts, err := optionsParam(params, 1, true)
		if err != nil {
			return nil, err
		}
		return h.engine.ChangeOption(gid, opts)
	case "aria2.getGlobalOption":
		return h.engine.GetGlobalOption(), nil
	case "aria2.changeGlobalOption":
		opts, err := optionsParam(params, 0, true)
		if err != nil {
			return nil, err
		}
		return h.engine.ChangeGlobalOption(opts)
	case "aria2.getGlobalStat":
		return h.engine.GetGlobalStat(), nil
	case "aria2.purgeDownloadResult":
		return h.engine.PurgeDownloadResult()
	case "aria2.removeDownloadResult":
		gid, err := stringParam(params, 0)
		if err != nil {
			return nil, err
		}
		return h.engine.RemoveDownloadResult(gid)
	case "aria2.getVersion":
		return h.engine.GetVersion(), nil
	case "aria2.getSessionInfo":
		return h.engine.GetSessionInfo(), nil
	case "aria2.shutdown":
		h.engine.RequestShutdown(false)
		return "OK", nil
	case "aria2.forceShutdown":
		h.engine.RequestShutdown(true)
		return "OK", nil
	case "aria2.saveSession":
		return h.engine.SaveSession()
	case "system.multicall":
		return h.multicall(params)
	case "system.listMethods":
		return append([]string(nil), rpcMethods...), nil
	case "system.listNotifications":
		return append([]string(nil), notificationMethods...), nil
	default:
		return nil, callError{code: rpcMethodNotFound, msg: "Method not found"}
	}
}

func (h *RPCHandler) multicall(params []json.RawMessage) (any, error) {
	if len(params) < 1 {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	var calls []struct {
		MethodName string            `json:"methodName"`
		Params     []json.RawMessage `json:"params"`
	}
	if err := decodeUseNumber(params[0], &calls); err != nil {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	out := make([]any, 0, len(calls))
	for _, c := range calls {
		p, err := h.stripSecret(c.MethodName, c.Params)
		if err != nil {
			out = append(out, map[string]any{"faultCode": rpcAriaError, "faultString": err.Error()})
			continue
		}
		result, err := h.invoke(c.MethodName, p)
		if err != nil {
			out = append(out, map[string]any{"faultCode": rpcCode(err), "faultString": err.Error()})
			continue
		}
		out = append(out, []any{result})
	}
	return out, nil
}

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

func optionsParam(params []json.RawMessage, i int, required bool) (Options, error) {
	if i >= len(params) {
		if required {
			return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
		}
		return Options{}, nil
	}
	var raw map[string]any
	if err := decodeUseNumber(params[i], &raw); err != nil {
		return nil, callError{code: rpcInvalidParams, msg: "Invalid params"}
	}
	return normalizeOptions(raw), nil
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
