package infinibox

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-resty/resty"
	log "github.com/sirupsen/logrus"
)

type Pool struct {
	ID                       int64         `json:"id"`
	Name                     string        `json:"name"`
	CreatedAt                int64         `json:"created_at"`
	UpdatedAt                int64         `json:"updated_at"`
	PhysicalCapacity         uint64        `json:"physical_capacity"`
	VirtualCapacity          uint64        `json:"virtual_capacity"`
	PhysicalCapacityWarning  int           `json:"physical_capacity_warning"`
	PhysicalCapacityCritical int           `json:"physical_capacity_critical"`
	State                    string        `json:"state"`
	ReservedCapacity         int64         `json:"reserved_capacity"`
	MaxExtend                int64         `json:"max_extend"`
	SsdEnabled               bool          `json:"ssd_enabled"`
	CompressionEnabled       bool          `json:"compression_enabled"`
	CapacitySavings          int64         `json:"capacity_savings"`
	VolumesCount             int64         `json:"volumes_count"`
	FilesystemsCount         int64         `json:"filesystems_count"`
	SnapshotsCount           int64         `json:"snapshots_count"`
	FilesystemSnapshotsCount int64         `json:"filesystem_snapshots_count"`
	AllocatedPhysicalSpace   uint64        `json:"allocated_physical_space"`
	FreePhysicalSpace        uint64        `json:"free_physical_space"`
	Owners                   []interface{} `json:"owners"`
	QosPolicies              []interface{} `json:"qos_policies"`
	EntitiesCount            int           `json:"entities_count"`
	FreeVirtualSpace         uint64        `json:"free_virtual_space"`
	TenantID                 int64         `json:"tenant_id,omitempty"`
}

func (c *Client) GetPoolByName(poolname string) (*Pool, error) {

	queryRes, err := c.Find("pools", "name", "eq", poolname)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("cannot find pool by name: %s, error: %s", poolname, err.Error()))
	}

	if queryRes == nil {
		return nil, nil
	}

	var pools []Pool

	err = json.Unmarshal(*queryRes, &pools)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("unable to decode pool: %s query result, error: %s", poolname, err.Error()))
	}

	if len(pools) == 0 {
		return nil, errors.New(fmt.Sprintf("pool %s not found", poolname))
	}

	log.Debugf("Found pool %#v", &pools[0])

	return &pools[0], nil
}

func (c *Client) GetAllPools() (*[]Pool, error) {

	log.Debug("Getting pools collection")

	url := "api/rest/pools"

	var request *resty.Request

	if c.config.Tenant != "" {
		log.Debugf("Adding tenant_id %d to request", c.config.Tenant)
		request = c.RestClient.R().SetHeader("X-INFINIDAT-TENANT-ID", c.config.Tenant)
	} else {
		request = c.RestClient.R()
	}

	response, err := request.Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, errors.New("error getting pools collection")
	}

	if num := result.ApiMetadata["number_of_objects"]; num == nil {
		return nil, errors.New("cannot parse metadata for number_of_objects field")
	} else {
		if num == float64(0) {
			log.Infof("pools collection is empty")
			return nil, nil
		}
	}

	var pools []Pool
	err = json.Unmarshal(*result.ApiResult, &pools)
	if err != nil {
		return nil, errors.New("error getting pools collection")
	}

	log.Debugf("Got pools collection")

	return &pools, nil
}

func (c *Client) GetPool(poolID int64) (*Pool, error) {

	log.Debugf("Getting host object ID: %s", poolID)

	url := fmt.Sprintf("api/rest/hosts/%d", poolID)

	response, err := c.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, err
	}

	var pool Pool
	err = json.Unmarshal(*result.ApiResult, &pool)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("json: %s", err.Error()))
	}

	log.Debugf("Got pool object: %#v", pool)

	return &pool, nil
}

func (p *Pool) Create(client *Client) (err error) {

	log.Debugf("Creating pool: %s", p.Name)
	url := "api/rest/pools"

	var request *resty.Request

	if client.config.Tenant != "" {
		log.Debugf("Adding tenant_id %d to request", client.config.Tenant)
		request = client.RestClient.R().SetHeader("X-INFINIDAT-TENANT-ID", client.config.Tenant)
	} else {
		request = client.RestClient.R()
	}

	response, err := request.SetBody(map[string]interface{}{
		"name":              p.Name,
		"physical_capacity": p.PhysicalCapacity,
		"virtual_capacity":  p.VirtualCapacity}).Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return errors.New(fmt.Sprintf("error creating pool: %s,  %v", p.Name, err))
	}

	err = json.Unmarshal(*result.ApiResult, &p)
	if err != nil {
		return errors.New(fmt.Sprintf("error creating pool: %s,  %v", p.Name, err))
	}

	log.Debugf("Successfully created pool %s", p.Name)

	return nil
}

func (p *Pool) Delete(client *Client) (pool *Pool, err error) {

	log.Debugf("Deleting pool: %s", p.Name)
	url := fmt.Sprintf("api/rest/pools/%d", p.ID)
	response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error deleting pool: %s,  %v", p.Name, err))
	}

	err = json.Unmarshal(*result.ApiResult, &pool)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error deleting pool: %s,  %v", p.Name, err))
	}

	log.Debugf("Successfully deleted pool %s", p.Name)

	return pool, nil
}

func (p *Pool) updateAttributes(client *Client, attributesMap map[string]interface{}) (err error) {

	log.Debugf("Updating pool: %s", p.Name)
	url := fmt.Sprintf("api/rest/pools/%d", p.ID)

	if len(attributesMap) > 0 {
		response, err := client.RestClient.R().SetBody(attributesMap).Put(url)

		result, err := CheckAPIResponse(response, err)
		if err != nil {
			return errors.New(fmt.Sprintf("error updating pool: %s,  %v", p.Name, err))
		}

		err = json.Unmarshal(*result.ApiResult, &p)
		if err != nil {
			return errors.New(fmt.Sprintf("error updating pool: %s,  %v", p.Name, err))
		}
	}

	log.Debugf("Successfully updated pool %s", p.Name)

	return nil
}

func (p *Pool) UpdateName(client *Client, name string) error {

	log.Debugf("Renaming pool %s", p.Name)

	attributesMap := map[string]interface{}{"name": name}
	err := p.updateAttributes(client, attributesMap)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to rename pool %s, %s", p.Name, err.Error()))
	}

	log.Debugf("Succesfully renamed pool %s to %s", p.Name, name)

	return nil
}

func (p *Pool) UpdatePhysicalCapacity(client *Client, capacity uint64) error {

	log.Debugf("Updating PhysicalCapacity for pool %s", p.Name)

	attributesMap := map[string]interface{}{"physical_capacity": capacity}
	err := p.updateAttributes(client, attributesMap)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to update pool %s PhysicalCapacity, %s", p.Name, err.Error()))
	}

	log.Debugf("Succesfully updated pool %s PhysicalCapacity to %d", p.Name, capacity)

	return nil
}

func (p *Pool) UpdateVirtualCapacity(client *Client, capacity uint64) error {

	log.Debugf("Updating VirtualCapacity for pool %s", p.Name)

	attributesMap := map[string]interface{}{"virtual_capacity": capacity}
	err := p.updateAttributes(client, attributesMap)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to update pool %s VirtualCapacity, %s", p.Name, err.Error()))
	}

	log.Debugf("Succesfully updated pool %s VirtualCapacity to %d", p.Name, capacity)

	return nil
}

func (p *Pool) UpdateSsdEnabled(client *Client, enabled bool) error {

	log.Debugf("Updating SsdEnabled for pool %s", p.Name)

	attributesMap := map[string]interface{}{"ssd_enabled": enabled}
	err := p.updateAttributes(client, attributesMap)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to update pool %s SsdEnabled, %s", p.Name, err.Error()))
	}

	log.Debugf("Succesfully updated pool %s SsdEnabled to %v", p.Name, enabled)

	return nil
}

func (p *Pool) UpdateCompressionEnabled(client *Client, enabled bool) error {

	log.Debugf("Updating CompressionEnabled for pool %s", p.Name)

	attributesMap := map[string]interface{}{"compression_enabled": enabled}
	err := p.updateAttributes(client, attributesMap)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to update pool %s CompressionEnabled, %s", p.Name, err.Error()))
	}

	log.Debugf("Succesfully updated pool %s CompressionEnabled to %v", p.Name, enabled)

	return nil
}
