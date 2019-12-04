package infinibox

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty"
	log "github.com/sirupsen/logrus"
)

type Initiator struct {
	HostID  int64   `json:"host_id"`
	PortKey float64 `json:"port_key"`
	Type    string  `json:"type"`
	Targets []struct {
		NodeID    int    `json:"node_id"`
		SessionID int    `json:"session_id,omitempty"`
		Address   string `json:"address"`
	} `json:"targets"`
	Address string `json:"address"`
}

func (c *Client) GetAllInitiators() (initiators *[]Initiator, err error) {

	log.Debug("Getting all initiators")

	url := "api/rest/initiators"
	var request *resty.Request

	if c.config.Tenant != "" {
		log.Debugf("Adding tenant_id %d to request", &c.config.Tenant)
		request = c.RestClient.R().SetQueryParam("tenant_id", c.config.Tenant)
	} else {
		request = c.RestClient.R()
	}

	response, err := request.Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting all initiators %s", err.Error()))
	}

	err = json.Unmarshal(*result.APIResult, &initiators)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting all initiators, error: %s", err.Error()))
	}

	return initiators, nil
}

func (c *Client) GetInitiatorByAddress(address string) (initiator *Initiator, err error) {

	log.Debugf("Getting initiator by address: %s", address)

	url := fmt.Sprintf("api/rest/initiators/%s", address)

	var request *resty.Request

	if c.config.Tenant != "" {
		log.Debugf("Adding tenant_id %d to request", &c.config.Tenant)
		request = c.RestClient.R().SetHeader("X-INFINIDAT-TENANT-ID", c.config.Tenant)
	} else {
		request = c.RestClient.R()
	}

	response, err := request.Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting initiator by address %s", err.Error()))
	}

	err = json.Unmarshal(*result.APIResult, &initiator)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting initiator by address, error: %s", err.Error()))
	}

	return initiator, nil
}
