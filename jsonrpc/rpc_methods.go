package jsonrpc

import "encoding/json"

var notificationMethods = []string{
	"aria2.onDownloadStart",
	"aria2.onDownloadPause",
	"aria2.onDownloadStop",
	"aria2.onDownloadComplete",
	"aria2.onDownloadError",
	"aria2.onBtDownloadComplete",
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
