package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"

	"github.com/gorilla/websocket"
	"github.com/workflow-interoperability/bulk-buyer/lib"
	"github.com/workflow-interoperability/bulk-buyer/types"
	"github.com/zeebe-io/zeebe/clients/go/entities"
	"github.com/zeebe-io/zeebe/clients/go/worker"
)

// ReceiveWaybillWorker receive order
func ReceiveWaybillWorker(client worker.JobClient, job entities.Job) {
	processID := "special-carrier"
	iesmid := "4"
	jobKey := job.GetKey()
	log.Println("Start receive waybill " + strconv.Itoa(int(jobKey)))
	payload, err := job.GetVariablesAsMap()
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}

	// waiting for IM from sender
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:3004", Path: ""}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	defer c.Close()
	for {
		finished := false
		_, msg, err := c.ReadMessage()
		if err != nil {
			log.Println(err)
			lib.FailJob(client, job)
			return
		}
		// check message type and handle
		var structMsg map[string]interface{}
		err = json.Unmarshal(msg, &structMsg)
		if err != nil {
			log.Println(err)
			lib.FailJob(client, job)
			return
		}
		switch structMsg["$class"].(string) {
		case "org.sysu.wf.IMCreatedEvent":
			// get im
			processData, err := lib.GetIM("http://127.0.0.1:3003/api/IM/" + structMsg["id"].(string))
			if err != nil {
				continue
			}
			if !(processData.Payload.WorkflowRelevantData.From.ProcessInstanceID == payload["fromProcessInstanceID"].(map[string]interface{})["supplier"] && processData.Payload.WorkflowRelevantData.To.IESMID == iesmid && processData.Payload.WorkflowRelevantData.To.ProcessID == processID) {
				continue
			}
			// create piis
			id := lib.GenerateXID()
			newPIIS := types.PIIS{
				ID: id,
				From: types.FromToData{
					ProcessID:         processID,
					ProcessInstanceID: payload["processInstanceID"].(string),
					IESMID:            iesmid,
				},
				To: processData.Payload.WorkflowRelevantData.From,
				SubscriberInformation: types.SubscriberInformation{
					Roles: []string{},
					ID:    "supplier",
				},
			}
			pPIIS := types.PublishPIIS{newPIIS}
			body, err := json.Marshal(&pPIIS)
			if err != nil {
				log.Println(err)
				lib.FailJob(client, job)
				return
			}
			err = lib.BlockchainTransaction("http://127.0.0.1:3003/api/PublishPIIS", string(body))
			if err != nil {
				log.Println(err)
				lib.FailJob(client, job)
				return
			}
			finished = true
		}
		if finished {
			fmt.Println("Send PIIS success.")
			break
		}
	}
	request, err := client.NewCompleteJobCommand().JobKey(jobKey).VariablesFromMap(payload)
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	request.Send()
}
