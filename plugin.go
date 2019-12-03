package infinibox

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type Plugin struct {
	ID                     int64  `json:"id"`
	Type                   string `json:"type"`
	Name                   string `json:"name"`
	Version                string `json:"version"`
	APIRedirectSuffix      string `json:"api_redirect_suffix"`
	ManagementURL          string `json:"management_url"`
	MaxSecWithoutHeartbeat int    `json:"max_sec_without_heartbeat"`
	CreatedAt              int64  `json:"created_at"`
	UpdatedAt              int64  `json:"updated_at"`
	TenantID               int64  `json:"tenant_id"`
	Capacity               struct {
		TotalPhysicalCapacity  int64 `json:"total_physical_capacity"`
		AllocatedPhysicalSpace int64 `json:"allocated_physical_space"`
		TotalVirtualCapacity   int64 `json:"total_virtual_capacity"`
		AllocatedVirtualSpace  int64 `json:"allocated_virtual_space"`
	} `json:"capacity"`
	LastHeartbeat  int64     `json:"last_heartbeat"`
	HeartbeatValid bool      `json:"heartbeat_valid"`
	Heartbeat      Heartbeat `json:"heartbeat"`
}

type Heartbeat struct {
	EntityCounts []struct {
		Entity string `json:"entity"`
		Count  int64  `json:"count"`
	} `json:"entity_counts"`
	HealthState struct {
		State    string   `json:"state"`
		Messages []string `json:"messages"`
	} `json:"health_state"`
}

func (c *Client) GetPlugintByName(pluginname string) (*Plugin, error) {

	queryRes, err := c.Find("plugins", "name", "eq", pluginname)

	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("cannot find plugin by name: %s, error: %s", pluginname, err.Error()))
	}

	if queryRes == nil {
		return nil, nil
	}

	var plugins []Plugin

	err = json.Unmarshal(*queryRes, &plugins)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("unable to decode plugins collection: %s query result, error: %s", pluginname, err.Error()))
	}

	if len(plugins) == 0 {
		return nil, fmt.Errorf(fmt.Sprintf("plugin %s not found", pluginname))
	}

	log.Debugf("Found plugin %#v", &plugins[0])

	return &plugins[0], nil
}

func (p *Plugin) Create(client *Client) (err error) {

	log.Debugf("Creating plugin: %s", p.Name)
	url := "api/rest/plugins"
	response, err := client.RestClient.R().SetBody(map[string]interface{}{
		"name": p.Name}).Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error creating plugin: %s,  %v", p.Name, err))
	}

	err = json.Unmarshal(*result.ApiResult, &p)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error creating plugin: %s,  %v", p.Name, err))
	}

	log.Debugf("Successfully created plugin %s", p.Name)

	return nil
}

func (p *Plugin) Delete(client *Client) (plugin *Plugin, err error) {

	log.Debugf("Deleting tenant: %s", p.Name)
	url := fmt.Sprintf("api/rest/plugins/%d", p.ID)
	response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error deleting plugin: %s,  %v", p.Name, err))
	}

	err = json.Unmarshal(*result.ApiResult, &plugin)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error deleting plugin: %s,  %v", p.Name, err))
	}

	log.Debugf("Successfully deleted plugin %s", p.Name)

	return plugin, nil
}

func (p *Plugin) updateAttributes(client *Client, attributesMap map[string]interface{}) (err error) {

	log.Debugf("Updating plugin: %s", p.Name)
	url := fmt.Sprintf("api/rest/plugins/%d", p.ID)

	if len(attributesMap) > 0 {
		response, err := client.RestClient.R().SetBody(attributesMap).Put(url)

		result, err := CheckAPIResponse(response, err)
		if err != nil {
			return fmt.Errorf(fmt.Sprintf("error updating plugin: %s,  %v", p.Name, err))
		}

		err = json.Unmarshal(*result.ApiResult, &p)
		if err != nil {
			return fmt.Errorf(fmt.Sprintf("error updating plugin: %s,  %v", p.Name, err))
		}
	}

	log.Debugf("Successfully updated plugin %s", p.Name)

	return nil
}

func (p *Plugin) UpdateName(client *Client, name string) error {

	log.Debugf("Renaming plugin %s", p.Name)

	attributesMap := map[string]interface{}{"name": name}
	err := p.updateAttributes(client, attributesMap)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("failed to rename plugin %s, %s", p.Name, err.Error()))
	}

	log.Debugf("Succesfully renamed plugin %s to %s", p.Name, name)

	return nil
}

func (p *Plugin) SendPluginHeartbeat(client *Client, heartbeat Heartbeat) error {

	log.Debugf("Sending plugin heartbeat %s", p.Name)

	url := fmt.Sprintf("api/rest/plugins/%d/heartbeat", p.ID)

	response, err := client.RestClient.R().SetBody(heartbeat).Put(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error sending plugin heartbeat: %s,  %v", p.Name, err))
	}

	err = json.Unmarshal(*result.ApiResult, &heartbeat)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error sending plugin heartbeat: %s,  %v", p.Name, err))
	}

	log.Debugf("Succesfully sent plugin heartbeat %s to %s", p.Name, heartbeat)

	return nil
}
