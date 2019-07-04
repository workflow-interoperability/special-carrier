package main

import (
	"encoding/json"
	"log"
	"net/url"
	"os"

	"github.com/gorilla/websocket"
	"github.com/workflow-interoperability/special-carrier/lib"
	"github.com/workflow-interoperability/special-carrier/types"
	"github.com/workflow-interoperability/special-carrier/worker"
	"github.com/zeebe-io/zeebe/clients/go/zbc"
)

const brokerAddr = "127.0.0.1:26500"

var processID = "special-carrier"
var iesmid = "1"

func main() {
	client, err := zbc.NewZBClient(brokerAddr)
	if err != nil {
		panic(err)
	}

	stopChan := make(chan bool, 0)

	// define worker
	go func() {
		requestDetailsWorker := client.NewJobWorker().JobType("requestDetails").Handler(worker.RequestDetailsWorker).Open()
		defer requestDetailsWorker.Close()
		requestDetailsWorker.AwaitClose()
	}()
	go func() {
		receiveDetailsWorker := client.NewJobWorker().JobType("receiveDetails").Handler(worker.ReceiveDetailsWorker).Open()
		defer receiveDetailsWorker.Close()
		receiveDetailsWorker.AwaitClose()
	}()
	go func() {
		receiveWaybillWorker := client.NewJobWorker().JobType("receiveWaybill").Handler(worker.ReceiveWaybillWorker).Open()
		defer receiveWaybillWorker.Close()
		receiveWaybillWorker.AwaitClose()
	}()
	go func() {
		deliverOrderWorker := client.NewJobWorker().JobType("deliverOrder").Handler(worker.DeliverOrderWorker).Open()
		defer deliverOrderWorker.Close()
		deliverOrderWorker.AwaitClose()
	}()

	// listen to blockchain event
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:3002", Path: ""}
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer c.Close()

	go func() {
		for {
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
				return
			}
			switch structMsg["$class"].(string) {
			case "org.sysu.wf.IMCreatedEvent":
				createSellerWorkflowInstance(structMsg["id"].(string), processID, iesmid, client)
			}
		}
	}()

	<-stopChan
}

// TODO: add manufacturer information
func createSellerWorkflowInstance(imID, processID, iermID string, client zbc.ZBClient) {
	// get im
	imData, err := lib.GetIM("http://127.0.0.1:3003/api/IM/" + imID)
	if err != nil {
		return
	}
	if !(imData.Payload.WorkflowRelevantData.To.ProcessID == processID && imData.Payload.WorkflowRelevantData.To.IESMID == iermID) {
		return
	}

	id := lib.GenerateXID()
	// publish blockchain asset
	var data map[string]interface{}
	if imData.Payload.ApplicationData.URL != "" {
		err = json.Unmarshal([]byte(imData.Payload.ApplicationData.URL), &data)
		if err != nil {
			log.Println(err)
			return
		}
	}
	// parse application.url
	var url map[string]interface{}
	err = json.Unmarshal([]byte(imData.Payload.ApplicationData.URL), &url)
	if err != nil {
		log.Println(err)
		return
	}
	data["fromProcessInstanceID"] = map[string]string{}
	data["fromProcessInstanceID"].(map[string]string)["middleman"] = imData.Payload.WorkflowRelevantData.From.ProcessInstanceID
	data["fromProcessInstanceID"].(map[string]string)["manufacturer"] = url["fromProcessInstanceID"].(map[string]interface{})["manufacturer"].(string)
	data["fromProcessInstanceID"].(map[string]string)["supplier"] = url["fromProcessInstanceID"].(map[string]interface{})["supplier"].(string)
	data["processInstanceID"] = id
	// create piis
	newPIIS := types.PIIS{
		ID: id,
		From: types.FromToData{
			ProcessID:         processID,
			ProcessInstanceID: id,
			IESMID:            iesmid,
		},
		To: imData.Payload.WorkflowRelevantData.From,
		SubscriberInformation: types.SubscriberInformation{
			Roles: []string{},
			ID:    "middleman",
		},
	}
	pPIIS := types.PublishPIIS{newPIIS}
	body, err := json.Marshal(&pPIIS)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	err = lib.BlockchainTransaction("http://127.0.0.1:3003/api/PublishPIIS", string(body))
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	log.Println("Publish PIIS success")
	// add workflow instance
	request, err := client.NewCreateInstanceCommand().BPMNProcessId(processID).LatestVersion().VariablesFromMap(data)
	if err != nil {
		log.Println(err)
		return
	}
	request.Send()
}
