package infinibox

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/go-resty/resty"
	log "github.com/sirupsen/logrus"
)

type HostCluster struct {
	Luns          []Lun  `json:"luns"`
	Name          string `json:"name"`
	CreatedAt     uint64 `json:"created_at"`
	HostType      string `json:"host_type"`
	UpdatedAt     uint64 `json:"updated_at"`
	SanClientType string `json:"san_client_type"`
	Hosts         []Host `json:"hosts"`
	ID            int64  `json:"id"`
	TenantID      int64  `json:"tenant_id,omitempty"`
	mu            sync.Mutex
}

func (c *Client) GetHostClusterByName(clustername string) (*HostCluster, error) {

	log.Infof("querying host cluster by name: %s", clustername)

	queryRes, err := c.Find("clusters", "name", "eq", clustername)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("cannot find hostc luster: %s, error: %s", clustername, err.Error()))
	}

	if queryRes == nil {
		return nil, nil
	}

	var hostclusters []HostCluster

	err = json.Unmarshal(*queryRes, &hostclusters)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("unable to decode host cluster: %s query result, error: %s", clustername, err.Error()))
	}

	if len(hostclusters) == 0 {
		return nil, errors.New(fmt.Sprintf("host cluster %s not found", clustername))
	}

	return &hostclusters[0], nil
}

func (c *Client) GetAllHostClusters() (*[]HostCluster, error) {

	log.Debug("Getting host clusters collection")

	url := "api/rest/clusters"
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
		return nil, errors.New("error getting hosts collection")
	}

	if num := result.ApiMetadata["number_of_objects"]; num == nil {
		return nil, errors.New("cannot parse metadata for number_of_objects field")
	} else {
		if num == float64(0) {
			log.Infof("host clusters collection is empty")
			return nil, nil
		}
	}

	var hosts []HostCluster
	err = json.Unmarshal(*result.ApiResult, &hosts)
	if err != nil {
		return nil, errors.New("error getting host clusters collection")
	}

	log.Debugf("Successfully fetched host clusters collection")
	return &hosts, nil
}

func (hc *HostCluster) Create(client *Client) (err error) {

	log.Debugf("Creating host cluster: %s", hc.Name)

	body := map[string]interface{}{"name": hc.Name}

	url := "api/rest/clusters"
	var request *resty.Request

	if client.config.Tenant != "" {
		log.Debugf("Adding tenant_id %d to request", client.config.Tenant)
		request = client.RestClient.R().SetHeader("X-INFINIDAT-TENANT-ID", client.config.Tenant)
	} else {
		request = client.RestClient.R()
	}

	response, err := request.SetBody(body).Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return errors.New(fmt.Sprintf("error creating host cluster: %s,  %s", hc.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &hc)
	if err != nil {
		return errors.New(fmt.Sprintf("error creating host cluster: %s,  %s", hc.Name, err.Error()))
	}

	log.Debugf("Successfully created host cluster %s", hc.Name)
	return nil
}

func (hc *HostCluster) Delete(client *Client) (err error) {

	log.Infof("Deleting host cluster: %s", hc.Name)

	url := fmt.Sprintf("api/rest/clusters/%d", hc.ID)
	response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return errors.New(fmt.Sprintf("error deleting host cluster: %s,  %s", hc.Name, err.Error()))
	}

	var hostcluster HostCluster
	err = json.Unmarshal(*result.ApiResult, &hostcluster)
	if err != nil {
		return errors.New(fmt.Sprintf("error deleting host cluster: %s,  %s", hc.Name, err.Error()))
	}

	log.Debugf("Successfully deleted host cluster %s", hc.Name)
	return nil
}

func (hc *HostCluster) Get(client *Client) (host *Host, err error) {

	log.Infof("Getting host: %s", hc.Name)

	url := fmt.Sprintf("api/rest/clusters/%d", hc.ID)
	response, err := client.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error getting host cluster: %s,  %s", hc.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &host)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error getting host cluster: %s,  %s", hc.Name, err.Error()))
	}

	log.Debugf("Successfully fetched host cluster %s", hc.Name)
	return host, nil
}

func (hc *HostCluster) AddHost(client *Client, hostID uint64) (err error) {

	log.Debugf("Adding hostID %d to host cluster: %s", hostID, hc.Name)

	body := map[string]interface{}{}
	body["id"] = hostID

	url := fmt.Sprintf("api/rest/clusters/%d/hosts", hc.ID)
	response, err := client.RestClient.R().SetBody(body).Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return errors.New(fmt.Sprintf("error adding hostID %d to host cluster: %s %s", hostID, hc.Name, err.Error()))
	}

	var newport Port
	err = json.Unmarshal(*result.ApiResult, &newport)
	if err != nil {
		return errors.New(fmt.Sprintf("error adding hostID %d to host cluster: %s %s", hostID, hc.Name, err.Error()))
	}

	log.Debugf("Successfully added hostID %d to host cluster %s", hostID, hc.Name)
	return nil
}

func (hc *HostCluster) GetHosts(client *Client) (hosts *[]Host, err error) {

	log.Debugf("Getting host cluster: %s hosts", hc.Name)

	url := fmt.Sprintf("api/rest/clusters/%d/hosts", hc.ID)
	response, err := client.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error getting host cluster: %s hosts,  %s", hc.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &hosts)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error getting host cluster: %s hosts,  %s", hc.Name, err.Error()))
	}

	log.Debugf("Successfully fetched host cluster %s hosts", hc.Name)
	return hosts, nil
}

func (hc *HostCluster) DeleteHost(client *Client, hostID uint64) (err error) {

	log.Debugf("Deleting hostID %d from host cluster: %s", hostID, hc.Name)

	url := fmt.Sprintf("api/rest/clusters/%d/hosts/%d", hc.ID, hostID)
	response, err := client.RestClient.R().Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return errors.New(fmt.Sprintf("error removing hostID %d from host cluster: %s %s", hostID, hc.Name, err.Error()))
	}

	var newport Port
	err = json.Unmarshal(*result.ApiResult, &newport)
	if err != nil {
		return errors.New(fmt.Sprintf("error removing hostID %d from host cluster: %s %s", hostID, hc.Name, err.Error()))
	}

	log.Debugf("Successfully deleted hostID %d from host cluster %s", hostID, hc.Name)
	return nil
}

func (hc *HostCluster) AddLUN(client *Client, lun *Lun) (err error) {

	log.Debugf("Adding volume_id: %d as lun to host cluster: %s", lun.VolumeID, hc.Name)

	body := map[string]interface{}{}

	body["volume_id"] = lun.VolumeID
	if lun.Lun > 0 {
		body["lun"] = lun.Lun
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()

	url := fmt.Sprintf("api/rest/clusters/%d/luns", hc.ID)
	response, err := client.RestClient.R().SetBody(body).SetQueryParam("approved", "true").Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return errors.New(fmt.Sprintf("error adding lun to host cluster: %s %s", hc.Name, err.Error()))
	}

	var newlun Lun
	err = json.Unmarshal(*result.ApiResult, &newlun)
	if err != nil {
		return errors.New(fmt.Sprintf("error adding lun to host cluster: %s %s", hc.Name, err.Error()))
	}

	log.Debugf("Successfully added new LUN %+v to host cluster %s", newlun, hc.Name)
	return nil
}

func (hc *HostCluster) GetLUNs(client *Client) (luns *[]Lun, err error) {

	log.Debugf("Getting host cluster: %s luns", hc.Name)

	hc.mu.Lock()
	defer hc.mu.Unlock()

	url := fmt.Sprintf("api/rest/clusters/%d/luns", hc.ID)
	response, err := client.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error getting host cluster: %s luns,  %s", hc.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &luns)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error getting host cluster: %s luns,  %s", hc.Name, err.Error()))
	}

	log.Debugf("Successfully fetched host cluster %s LUNs", hc.Name)
	return luns, nil
}

func (hc *HostCluster) DeleteLUN(client *Client, lunID int) (lun *Lun, err error) {

	log.Debugf("Deleting host cluster: %s lun ID %d", hc.Name, lunID)

	url := fmt.Sprintf("api/rest/clusters/%d/luns/lun/%d", hc.ID, lunID)
	response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error deleting host cluster: %s lun ID %d ,  %s", hc.Name, lunID, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &lun)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error deleting host cluster: %s lun ID %d,  %s", hc.Name, lunID, err.Error()))
	}

	log.Debugf("Successfully deleted host cluster %s LUN %d", hc.Name, lunID)
	return lun, nil
}

func (hc *HostCluster) SetMetadata(client *Client, key string, value string) (err error) {

	log.Debugf("Setting metadata for host cluster %s", hc.Name)

	err = client.AddMetadata(&Metadata{ObjectID: hc.ID, Key: key, Value: value})
	if err != nil {
		return errors.New(fmt.Sprintf("unable to set metadata for host cluster %s, error %s", hc.Name, err.Error()))
	}

	return nil
}

func (hc *HostCluster) GetMetadata(client *Client, key string) (metadata *[]Metadata, err error) {

	log.Debugf("Setting metadata for host cluster %s", hc.Name)

	metadata, err = client.GetMetadataByObject(hc.ID)
	if err != nil {
		return metadata, errors.New(fmt.Sprintf("unable to get metadata for host cluster %s, error %s", hc.Name, err.Error()))
	}

	return metadata, nil
}

func (hc *HostCluster) GetMetadataValue(client *Client, key string) (value interface{}, err error) {

	log.Debugf("Setting metadata for host cluster %s", hc.Name)

	metadata, err := client.GetMetadataByObjectAndKey(hc.ID, key)
	if err != nil {
		return value, errors.New(fmt.Sprintf("unable to get metadata for host cluster %s, error %s", hc.Name, err.Error()))
	}

	value = metadata.Value

	return value, nil
}

func (hc *HostCluster) UnSetMetadata(client *Client, key string) (err error) {

	log.Debugf("Setting metadata for host cluster %s", hc.Name)

	err = client.DeleteMetadataByKey(hc.ID, key)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to unset metadata for host cluster %s, error %s", hc.Name, err.Error()))
	}

	return nil
}

func (hc *HostCluster) ClearMetadata(client *Client) (err error) {

	log.Debugf("Setting metadata for host cluster %s", hc.Name)

	err = client.DeleteMetadata(hc.ID)
	if err != nil {
		return errors.New(fmt.Sprintf("unable to clear metadata for host %s, error %s", hc.Name, err.Error()))
	}

	return nil
}
