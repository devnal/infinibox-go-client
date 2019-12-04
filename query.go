package infinibox

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty"
	log "github.com/sirupsen/logrus"
)

func (c *Client) Find(collection string, param string, op string, value string) (queryRes *json.RawMessage, err error) {

	url := fmt.Sprintf("/api/rest/%s", collection)

	var request *resty.Request

	if c.config.Tenant != "" {
		log.Debugf("Adding tenant_id %d to request", c.config.Tenant)
		request = c.RestClient.R().SetQueryParam("tenant_id", c.config.Tenant)
	} else {
		request = c.RestClient.R()
	}

	response, err := request.SetQueryParam(param, fmt.Sprint(op+string(':')+value)).Get(url)

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	apiResult, err := CheckAPIResponse(response, err)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	queryRes = apiResult.APIResult

	return queryRes, nil
}
