package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/logp"
	"github.com/elastic/libbeat/outputs"
)

type HTTPOutput struct {
	URL string
}

func (out *HTTPOutput) Init(config outputs.MothershipConfig, topology_expire int) error {

	out.URL = fmt.Sprintf("http://%s:%d", config.Host, config.Port)
	logp.Info("[HTTPOutput] Flushing data to %s", out.URL)
	return nil
}

func (out *HTTPOutput) GetNameByIP(ip string) string {
	logp.Info("NOT IMPLEMENTED")
	return ""
}

func (out *HTTPOutput) PublishIPs(name string, localAddrs []string) error {
	logp.Info("NOT IMPLEMENTED")
	return nil
}

func (out *HTTPOutput) PublishEvent(ts time.Time, event common.MapStr) error {
	// Normalize stuff
	spanReadyEvent := common.MapStr{}

	startTs := event["timestamp"].(common.Time)
	spanReadyEvent["start"] = float64(time.Time(startTs).UnixNano()) / 1e9
	spanType := event["type"].(string)
	spanReadyEvent["type"] = spanType
	spanReadyEvent["duration"] = float64(event["responsetime"].(int32)) / 1e6

	metaToSave := []string{"bytes_in", "bytes_out", "client_ip", "client_port", "port", "status", "resource", "method", "query", "proc", "client_proc"}
	metaMap := map[string]string{}

	for _, v := range metaToSave {
		metastr, ok := event[v].(string)
		if !ok {
			metaMap[v] = fmt.Sprintf("%v", event[v])
		} else {
			metaMap[v] = metastr
		}

	}

	// Flatten some other proto specific meta
	for k, _ := range event[spanType].(common.MapStr) {
		metakey := spanType + "_" + k
		metastr, ok := event[spanType].(common.MapStr)[k].(string)
		if !ok {
			metaMap[metakey] = fmt.Sprintf("%v", event[spanType].(common.MapStr)[k])
		} else {
			metaMap[metakey] = metastr
		}
	}

	spanReadyEvent["meta"] = metaMap

	json_event, err := json.Marshal(event)
	json_span, err := json.Marshal(spanReadyEvent)

	if err != nil {
		logp.Err("Fail to convert the event to JSON: %s", err)
		return err
	}

	logp.Info(string(json_event))
	logp.Info(string(json_span))

	resp, err := http.Post(out.URL, "application/json", bytes.NewBuffer(json_span))
	if err != nil {
		logp.Err("Could not post to local trace collector")
	} else {
		logp.Info("resp status:", resp.Status)
	}

	return nil
}
