package worker

import (
	"encoding/json"
	"log"
	"net/url"
	"strconv"

	"github.com/gorilla/websocket"
	"github.com/workflow-interoperability/special-carrier/lib"
	"github.com/workflow-interoperability/special-carrier/types"
	"github.com/zeebe-io/zeebe/clients/go/entities"
	"github.com/zeebe-io/zeebe/clients/go/worker"
)

// DeliverOrderWorker place order
func DeliverOrderWorker(client worker.JobClient, job entities.Job) {
	processID := "manufacturer"
	IESMID := "5"
	jobKey := job.GetKey()
	log.Println("Start place order " + strconv.Itoa(int(jobKey)))

	payload, err := job.GetVariablesAsMap()
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}

	// create blockchain IM instance
	id := lib.GenerateXID()
	aData, err := json.Marshal(&payload)
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	// TODO: add manufacturer information from middleman
	newIM := types.IM{
		ID: id,
		Payload: types.Payload{
			ApplicationData: types.ApplicationData{
				URL: string(aData),
			},
			WorkflowRelevantData: types.WorkflowRelevantData{
				From: types.FromToData{
					ProcessID:         processID,
					ProcessInstanceID: payload["processInstanceID"].(string),
					IESMID:            IESMID,
				},
				To: types.FromToData{
					ProcessID:         "manufacturer",
					ProcessInstanceID: payload["fromProcessInstanceID"].(map[string]string)["manufacturer"],
					IESMID:            "3",
				},
			},
		},
		SubscriberInformation: types.SubscriberInformation{
			Roles: []string{},
			ID:    "manufacturer",
		},
	}
	pim := types.PublishIM{IM: newIM}
	body, err := json.Marshal(&pim)
	if err != nil {
		log.Println(err)
		return
	}
	err = lib.BlockchainTransaction("http://127.0.0.1:3003/api/PublishIM", string(body))
	if err != nil {
		log.Println(err)
		lib.FailJob(client, job)
		return
	}
	payload["processID"] = id
	log.Println("Publish IM success")

	// waiting for PIIS from receiver
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:3001", Path: ""}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer c.Close()
	for {
		finished := false
		_, msg, err := c.ReadMessage()
		if err != nil {
			log.Println(err)
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
		case "org.sysu.wf.PIISCreatedEvent":
			if ok, err := publishPIIS(structMsg["id"].(string), &newIM, c); err != nil {
				lib.FailJob(client, job)
				return
			} else if ok {
				finished = true
				break
			}
		default:
			continue
		}
		if finished {
			log.Println("Publish PIIS success")
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
