package infinibox

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty"
	log "github.com/sirupsen/logrus"
)

type Port struct {
	HostID  int64  `json:"host_id"`
	Type    string `json:"type"`
	Address string `json:"address"`
}

type Lun struct {
	ID            int64 `json:"id"`
	Lun           int   `json:"lun"`
	Clustered     bool  `json:"clustered"`
	HostClusterID int64 `json:"host_cluster_id"`
	VolumeID      int64 `json:"volume_id"`
	HostID        int64 `json:"host_id"`
}

type Host struct {
	Luns                          []Lun  `json:"luns"`
	SecurityMethod                string `json:"security_method"`
	SecurityChapInboundUsername   string `json:"security_chap_inbound_username"`
	Name                          string `json:"name"`
	CreatedAt                     int64  `json:"created_at"`
	HostType                      string `json:"host_type"`
	UpdatedAt                     uint64 `json:"updated_at"`
	ID                            int64  `json:"id"`
	SanClientType                 string `json:"san_client_type"`
	SecurityChapHasOutboundSecret bool   `json:"security_chap_has_outbound_secret"`
	SecurityChapHasInboundSecret  bool   `json:"security_chap_has_inbound_secret"`
	HostClusterID                 int    `json:"host_cluster_id"`
	Ports                         []Port `json:"ports"`
	SecurityChapOutboundUsername  string `json:"security_chap_outbound_username"`
	SecurityChapInboundSecret     string `json:"security_chap_inbound_secret,omitempty"`
	SecurityChapOutboundSecret    string `json:"security_chap_outbound_secret,omitempty"`
	TenantID                      int64  `json:"tenant_id,omitempty"`
}

func (c *Client) GetHostByName(hostname string) (*Host, error) {

	queryRes, err := c.Find("hosts", "name", "eq", hostname)

	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("cannot find hostname: %s, error: %s", hostname, err.Error()))
	}

	if queryRes == nil {
		return nil, nil
	}

	var hosts []Host

	err = json.Unmarshal(*queryRes, &hosts)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("unable to decode host: %s query result, error: %s", hostname, err.Error()))
	}

	if len(hosts) == 0 {
		return nil, fmt.Errorf(fmt.Sprintf("host %s not found", hostname))
	}

	log.Debugf("Found host object: %#v", &hosts[0])

	return &hosts[0], nil
}

func (c *Client) GetAllHosts() (*[]Host, error) {

	log.Debug("Getting hosts collection")

	url := "api/rest/hosts"

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
		return nil, fmt.Errorf("error getting hosts collection")
	}

	if num := result.ApiMetadata["number_of_objects"]; num == nil {
		return nil, fmt.Errorf("cannot parse metadata for number_of_objects field")
	} else {
		if num == float64(0) {
			log.Infof("hosts collection is empty")
			return nil, nil
		}
	}

	var hosts []Host
	err = json.Unmarshal(*result.ApiResult, &hosts)
	if err != nil {
		return nil, fmt.Errorf("error getting hosts collection")
	}

	log.Debugf("Got hosts collection")

	return &hosts, nil
}

func (c *Client) GetHost(hostID int64) (*Host, error) {

	log.Debugf("Getting host object ID: %s", hostID)

	url := fmt.Sprintf("api/rest/hosts/%d", hostID)

	response, err := c.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, err
	}

	var host Host
	err = json.Unmarshal(*result.ApiResult, &host)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("json: %s", err.Error()))
	}

	log.Debugf("Got host object: %#v", host)

	return &host, nil
}

func (c *Client) GetHostIDbyInitiatorAddress(address string) (ID int64, err error) {

	log.Debugf("Getting host ID by initiator addres: %s", address)

	//url := fmt.Sprintf("api/rest/hosts/host_id_by_initiator_address/%s", address)
	url := "api/rest/hosts"

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
		return -1, err
	}

	var hosts []Host

	err = json.Unmarshal(*result.ApiResult, &hosts)
	if err != nil {
		return -1, fmt.Errorf(fmt.Sprintf("json: %s", err.Error()))
	}

	var host Host

	for _, host = range hosts {
		for _, port := range host.Ports {
			if port.Address == address {
				log.Debugf("Found address")
				break
			}
		}
		break
	}
	ID = host.ID
	log.Debugf("Got host ID: %d for address %s", ID, address)

	return ID, nil
}

func (h *Host) Create(client *Client) (err error) {

	log.Debugf("Creating host: %s", h.Name)

	body := map[string]interface{}{"name": h.Name}

	if h.SecurityMethod != "" {
		body["security_method"] = h.SecurityMethod
	}
	if h.SecurityChapInboundUsername != "" {
		body["security_chap_inbound_username"] = h.SecurityChapInboundUsername
	}
	if h.SecurityChapInboundSecret != "" {
		body["security_chap_inbound_secret"] = h.SecurityChapInboundSecret
	}
	if h.SecurityChapOutboundUsername != "" {
		body["security_chap_outbound_username"] = h.SecurityChapOutboundUsername
	}
	if h.SecurityChapOutboundSecret != "" {
		body["security_chap_outbound_secret"] = h.SecurityChapOutboundSecret
	}
	url := "api/rest/hosts"

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
		return fmt.Errorf(fmt.Sprintf("error creating host: %s,  %s", h.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &h)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error creating host: %s,  %s", h.Name, err.Error()))
	}

	log.Debugf("Successfully created host %s", h.Name)

	return nil
}

func (h *Host) Delete(client *Client) (err error) {

	log.Debugf("Deleting host: %s", h.Name)

	url := fmt.Sprintf("api/rest/hosts/%d", h.ID)
	response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error deleting host: %s,  %s", h.Name, err.Error()))
	}

	var host Host
	err = json.Unmarshal(*result.ApiResult, &host)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error deleting host: %s,  %s", h.Name, err.Error()))
	}

	log.Debugf("Successfully deleted host %s", h.Name)

	return nil
}

func (h *Host) Get(client *Client) (host *Host, err error) {

	log.Debugf("Getting host: %s", h.Name)

	url := fmt.Sprintf("api/rest/hosts/%d", h.ID)
	response, err := client.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting host: %s,  %s", h.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &host)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting host: %s,  %s", h.Name, err.Error()))
	}

	log.Debugf("Successfully fetched host %s", h.Name)

	return host, nil
}

func (h *Host) GetPorts(client *Client) (ports *[]Port, err error) {

	log.Debugf("Getting host: %s ports", h.Name)

	url := fmt.Sprintf("api/rest/hosts/%d/ports", h.ID)
	response, err := client.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting host: %s ports,  %s", h.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &ports)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting host: %s ports,  %s", h.Name, err.Error()))
	}

	log.Debugf("Got host: %s ports", h.Name)

	return ports, nil
}

func (h *Host) Update(client *Client) (err error) {

	log.Debugf("Updating host: %s", h.Name)

	currentHost, err := h.Get(client)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("host update failed, error: %s", err.Error()))
	}
	body := map[string]interface{}{}

	if currentHost.Name != h.Name {
		body["name"] = h.Name
	} else {

		if h.SecurityMethod != "" {
			body["security_method"] = h.SecurityMethod
		}
		if h.SecurityChapInboundUsername != "" {
			body["security_chap_inbound_username"] = h.SecurityChapInboundUsername
		}
		if h.SecurityChapInboundSecret != "" {
			body["security_chap_inbound_secret"] = h.SecurityChapInboundSecret
		}
		if h.SecurityChapOutboundUsername != "" {
			body["security_chap_outbound_username"] = h.SecurityChapOutboundUsername
		}
		if h.SecurityChapOutboundSecret != "" {
			body["security_chap_outbound_secret"] = h.SecurityChapOutboundSecret
		}
	}

	url := fmt.Sprintf("api/rest/hosts/%d", h.ID)
	response, err := client.RestClient.R().SetBody(body).SetQueryParam("approved", "true").Put(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error updating host: %s,  %s", h.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &h)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error updating host: %s,  %s", h.Name, err.Error()))
	}

	log.Debugf("Updated host: %s", h.Name)

	return nil
}

func (h *Host) AddPort(client *Client, port *Port) (err error) {

	log.Debugf("Adding port type: %s address: %s to host: %s", port.Type, port.Address, h.Name)

	body := map[string]interface{}{}
	body["type"] = port.Type
	body["address"] = port.Address

	url := fmt.Sprintf("api/rest/hosts/%d/ports", h.ID)
	response, err := client.RestClient.R().SetBody(body).SetQueryParam("approved", "true").Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error adding port to host: %s %s", h.Name, err.Error()))
	}

	var newport Port
	err = json.Unmarshal(*result.ApiResult, &newport)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error adding port to host: %s %s", h.Name, err.Error()))
	}

	log.Debugf("Added port type: %s address: %s to host: %s", port.Type, port.Address, h.Name)

	return nil
}

func (h *Host) AddLUN(client *Client, lun *Lun) (err error) {

	log.Debugf("Adding volume_id: %d as lun to host: %s", lun.VolumeID, h.Name)

	body := map[string]interface{}{}

	body["volume_id"] = lun.VolumeID
	if lun.Lun > 0 {
		body["lun"] = lun.Lun
	}

	url := fmt.Sprintf("api/rest/hosts/%d/luns", h.ID)
	response, err := client.RestClient.R().SetBody(body).SetQueryParam("approved", "true").Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error adding lun to host: %s %s", h.Name, err.Error()))
	}

	var newlun Lun
	err = json.Unmarshal(*result.ApiResult, &newlun)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error adding lun to host: %s %s", h.Name, err.Error()))
	}

	log.Debugf("Added volume_id: %d as lun to host: %s", lun.VolumeID, h.Name)

	return nil
}

func (h *Host) GetLUNs(client *Client) (luns *[]Lun, err error) {

	log.Debugf("Getting host: %s luns", h.Name)

	url := fmt.Sprintf("api/rest/hosts/%d/luns", h.ID)
	response, err := client.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting host: %s luns,  %s", h.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &luns)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting host: %s luns,  %s", h.Name, err.Error()))
	}

	log.Debugf("Got host: %s luns", h.Name)

	return luns, nil
}

func (h *Host) GetLUN(client *Client, lunID int) (lun *Lun, err error) {

	log.Debugf("Getting host: %s lun ID %d", h.Name, lunID)

	url := fmt.Sprintf("api/rest/hosts/%d/luns/%d", h.ID, lunID)
	response, err := client.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting host: %s lun ID %d ,  %s", h.Name, lunID, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &lun)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error getting host: %s lun ID %d,  %s", h.Name, lunID, err.Error()))
	}

	log.Debugf("Got host: %s lun ID %d", h.Name, lunID)

	return lun, nil
}

func (h *Host) DeleteLUN(client *Client, lunID int) (lun *Lun, err error) {

	log.Debugf("Deleting Lun ID %d for host %s", lunID, h.Name)

	url := fmt.Sprintf("api/rest/hosts/%d/luns/lun/%d", h.ID, lunID)
	response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error deleting host: %s lun ID %d ,  %s", h.Name, lunID, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &lun)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error deleting host: %s lun ID %d,  %s", h.Name, lunID, err.Error()))
	}

	log.Debugf("Deleting Lun ID %d for host %s", lunID, h.Name)

	return lun, nil
}

func (h *Host) UnMapVolume(client *Client, volumeID uint64) (lun *Lun, err error) {

	log.Debugf("Unmapping volume ID: %d from host %s", volumeID, h.Name)

	url := fmt.Sprintf("api/rest/hosts/%d/luns/volume_id/%d", h.ID, volumeID)
	response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error umapping volume ID %d from host: %s, %s", volumeID, h.Name, err.Error()))
	}

	err = json.Unmarshal(*result.ApiResult, &lun)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error umapping volume ID %d from host: %s, %s", volumeID, h.Name, err.Error()))
	}

	log.Debugf("Unmapped volume ID: %d from host %s", volumeID, h.Name)
	return lun, nil
}

func (h *Host) SetMetadata(client *Client, key string, value string) (err error) {

	log.Debugf("Setting metadata for host %s", h.Name)

	err = client.AddMetadata(&Metadata{ObjectID: h.ID, Key: key, Value: value})
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("unable to set metadata for host %s, error %s", h.Name, err.Error()))
	}

	log.Debugf("Set metadata for host %s", h.Name)

	return nil
}

func (h *Host) GetMetadata(client *Client, key string) (metadata *[]Metadata, err error) {

	log.Debugf("Getting metadata for host %s", h.Name)

	metadata, err = client.GetMetadataByObject(h.ID)
	if err != nil {
		return metadata, fmt.Errorf(fmt.Sprintf("unable to get metadata for host %s, error %s", h.Name, err.Error()))
	}

	log.Debugf("Got metadata for host %s", h.Name)

	return metadata, nil
}

func (h *Host) GetMetadataValue(client *Client, key string) (value interface{}, err error) {

	log.Debugf("Getting metadata value for host %s and key %s", h.Name, key)

	metadata, err := client.GetMetadataByObjectAndKey(h.ID, key)
	if err != nil {
		return value, fmt.Errorf(fmt.Sprintf("unable to get metadata for host %s, error %s", h.Name, err.Error()))
	}

	value = metadata.Value

	log.Debugf("Got metadata value for host %s and key %s", h.Name, key)

	return value, nil
}

func (h *Host) UnSetMetadata(client *Client, key string) (err error) {

	log.Debugf("Setting metadata for host %s", h.Name)

	err = client.DeleteMetadataByKey(h.ID, key)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("unable to unset metadata for host %s, error %s", h.Name, err.Error()))
	}

	log.Debugf("Set metadata for host %s", h.Name)

	return nil
}

func (h *Host) ClearMetadata(client *Client) (err error) {

	log.Debugf("Clearing metadata for host %s", h.Name)

	err = client.DeleteMetadata(h.ID)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("unable to clear metadata for host %s, error %s", h.Name, err.Error()))
	}

	log.Debugf("Cleared metadata for host %s", h.Name)

	return nil
}
