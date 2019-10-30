package infinibox

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-resty/resty"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

//Volume represnts struct
type Volume struct {
	CgID                  int64       `json:"cg_id"`
	RmrTarget             bool        `json:"rmr_target"`
	UpdatedAt             uint64      `json:"updated_at"`
	LockState             string      `json:"lock_state"`
	NumBlocks             int64       `json:"num_blocks"`
	QosPolicyName         string      `json:"qos_policy_name"`
	Serial                string      `json:"serial"`
	ID                    int64       `json:"id"`
	QosSharedPolicyName   string      `json:"qos_shared_policy_name"`
	Size                  uint64      `json:"size"`
	SsdEnabled            bool        `json:"ssd_enabled"`
	ParentID              int64       `json:"parent_id"`
	CompressionSuppressed interface{} `json:"compression_suppressed"`
	Type                  string      `json:"type"`
	QosSharedPolicyID     int64       `json:"qos_shared_policy_id"`
	RmrSource             bool        `json:"rmr_source"`
	PoolName              string      `json:"pool_name"`
	Used                  uint64      `json:"used"`
	TreeAllocated         uint64      `json:"tree_allocated"`
	HasChildren           bool        `json:"has_children"`
	DatasetType           string      `json:"dataset_type"`
	Provtype              string      `json:"provtype"`
	QosPolicyID           int64       `json:"qos_policy_id"`
	RmrSnapshotGUID       string      `json:"rmr_snapshot_guid"`
	CapacitySavings       interface{} `json:"capacity_savings"`
	Name                  string      `json:"name"`
	DataSnapshotGUID      string      `json:"data_snapshot_guid"`
	Udid                  string      `json:"udid"`
	CreatedAt             uint64      `json:"created_at"`
	PoolID                int64       `json:"pool_id"`
	CompressionEnabled    bool        `json:"compression_enabled"`
	FamilyID              int         `json:"family_id"`
	Depth                 int         `json:"depth"`
	WriteProtected        bool        `json:"write_protected"`
	Mapped                bool        `json:"mapped"`
	Allocated             uint64      `json:"allocated"`
	LockExpiresAt         uint64      `json:"lock_expires_at"`
	TenantID              int64       `json:"tenant_id,omitempty"`
}

//GetVolumeByName get volume by name
func (c *Client) GetVolumeByName(volumename string) (*Volume, error) {

	queryRes, err := c.Find("volumes", "name", "eq", volumename)

	if err != nil {
		return nil, fmt.Errorf("cannot find volume by name: %s, error: %s", volumename, err.Error())
	}

	if queryRes == nil {
		return nil, nil
	}

	var volumes []Volume

	err = json.Unmarshal(*queryRes, &volumes)
	if err != nil {
		return nil, fmt.Errorf("unable to decode volume: %s query result, error: %s", volumename, err.Error())
	}

	if len(volumes) == 0 {
		return nil, fmt.Errorf("volume %s not found", volumename)
	}

	log.Debugf("Found volume %#v", &volumes[0])

	return &volumes[0], nil
}

//GetAllVolumes get all defined volumes
func (c *Client) GetAllVolumes() (*[]Volume, error) {

	log.Debug("Getting volumes collection")

	url := "api/rest/volumes"

	var request *resty.Request

	if c.config.Tenant != "" {
		log.Debugf("Adding tenant_id %s to request", c.config.Tenant)
		request = c.RestClient.R().SetHeader("X-INFINIDAT-TENANT-ID", c.config.Tenant)
	} else {
		request = c.RestClient.R()
	}

	response, err := request.Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, errors.New("error getting volumes collection")
	}

	if num := result.ApiMetadata["number_of_objects"]; num == nil {
		return nil, errors.New("cannot parse metadata for number_of_objects field")
	} else {
		if num == float64(0) {
			log.Infof("volumes collection is empty")
			return nil, nil
		}
	}

	var volumes []Volume
	err = json.Unmarshal(*result.ApiResult, &volumes)
	if err != nil {
		return nil, errors.New("error getting volumes collection")
	}

	log.Debugf("Got volumes collection")

	return &volumes, nil
}

//GetVolume get volume
func (c *Client) GetVolume(volumeID int64) (*Volume, error) {

	log.Debugf("Getting volume object ID: %d", volumeID)

	url := fmt.Sprintf("api/rest/volumes/%d", volumeID)
	response, err := c.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf("error getting volume object, %s", err.Error())
	}

	var volume Volume
	err = json.Unmarshal(*result.ApiResult, &volume)
	if err != nil {
		return nil, fmt.Errorf("error getting volume object %s", err.Error())
	}

	log.Debugf("Got volume object: %#v", volume)

	return &volume, nil
}

//Create volume create method
func (v *Volume) Create(client *Client) (err error) {

	log.Debugf("Creating volume: %s", v.Name)

	if v.Provtype == "" {
		v.Provtype = "THIN"
	}

	url := "api/rest/volumes"

	var request *resty.Request

	if client.config.Tenant != "" {
		log.Debugf("Adding tenant_id %s to request", client.config.Tenant)
		request = client.RestClient.R().SetHeader("X-INFINIDAT-TENANT-ID", client.config.Tenant)
	} else {
		request = client.RestClient.R()
	}

	response, err := request.SetBody(map[string]interface{}{
		"name":            v.Name,
		"pool_id":         v.PoolID,
		"size":            v.Size,
		"provtype":        v.Provtype,
		"write_protected": v.WriteProtected,
		"ssd_enabled":     v.SsdEnabled}).Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf("error creating volume: %s,  %s", v.Name, err.Error())
	}

	err = json.Unmarshal(*result.ApiResult, &v)
	if err != nil {
		return fmt.Errorf("error creating volume: %s,  %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully created volume %s", v.Name)
	return nil
}

//Get volume get
func (v *Volume) Get(client *Client) (volume *Volume, err error) {

	log.Debugf("Getting volume: %s", v.Name)

	url := fmt.Sprintf("api/rest/volumes/%d", v.ID)
	response, err := client.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf("error getting volume: %s,  %s", v.Name, err.Error())
	}

	err = json.Unmarshal(*result.ApiResult, &volume)
	if err != nil {
		return nil, fmt.Errorf("error getting volume: %s,  %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully fetched volume %s", v.Name)

	return volume, nil
}

//GetLUNs volume defines LUNs
func (v *Volume) GetLUNs(client *Client) (luns *[]Lun, err error) {

	log.Debugf("Getting volume: %s luns", v.Name)

	url := fmt.Sprintf("api/rest/volumes/%d/luns", v.ID)
	response, err := client.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf("error getting volume %s luns,  %s", v.Name, err.Error())
	}

	err = json.Unmarshal(*result.ApiResult, &luns)

	if err != nil {
		return nil, fmt.Errorf("error getting volume %s luns,  %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully fetched all LUNs information for volume %s", v.Name)

	return luns, nil
}

//UnMap volume unmap
func (v *Volume) UnMap(client *Client) (err error) {

	log.Debugf("Unmapping volume: %s luns", v.Name)

	currentVolume, err := v.Get(client)

	if err != nil {
		return fmt.Errorf("error unmapping volume %s luns, error: %s", v.Name, err.Error())
	}

	if currentVolume.Mapped {

		luns, err := currentVolume.GetLUNs(client)
		if err != nil {
			return fmt.Errorf("error unmapping volume %s luns, error: %s", v.Name, err.Error())
		}

		for _, lun := range *luns {
			if lun.Clustered && lun.HostID != 0 {
				log.Infof("unmapping host cluster LUN %+v from volume %s", lun, v.Name)
				url := fmt.Sprintf("api/rest/clusters/%d/luns/lun/%d", lun.HostClusterID, lun.Lun)
				response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

				result, err := CheckAPIResponse(response, err)
				if err != nil {
					return fmt.Errorf("error deleting host cluster: %d lun ID %d ,  %s", lun.HostClusterID, lun.Lun, err.Error())
				}

				var deletedLun Lun
				err = json.Unmarshal(*result.ApiResult, &deletedLun)
				if err != nil {
					return fmt.Errorf("error deleting host cluster: %d lun ID %d,  %s", lun.HostClusterID, lun.Lun, err.Error())
				}
				log.Infof("unmapped host cluster LUN %+v from volume %s", lun, v.Name)
			}
		}

		luns, err = currentVolume.GetLUNs(client)

		for _, lun := range *luns {
			log.Infof("unmapping host LUN %+v from volume %s", lun, v.Name)
			url := fmt.Sprintf("api/rest/hosts/%d/luns/lun/%d", lun.HostID, lun.Lun)
			response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

			result, err := CheckAPIResponse(response, err)
			if err != nil {
				return fmt.Errorf("error deleting host: %d lun ID %d ,  %s", lun.HostID, lun.Lun, err.Error())
			}

			var deletedLun Lun
			err = json.Unmarshal(*result.ApiResult, &deletedLun)
			if err != nil {
				return fmt.Errorf("error deleting host: %d lun ID %d,  %s", lun.HostID, lun.Lun, err.Error())
			}
			log.Infof("unmapped host LUN %+v from volume %s", lun, v.Name)
		}
	} else {
		log.Infof("volume %s is not mapped", v.Name)
	}

	log.Debugf("Succesfully unmapped volume %s", v.Name)

	return nil
}

//Delete volume delete
func (v *Volume) Delete(client *Client) (err error) {

	log.Debugf("Deleting volume: %s", v.Name)

	url := fmt.Sprintf("api/rest/volumes/%d", v.ID)
	response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf("error deleting volume: %s,  %s", v.Name, err.Error())
	}
	var volume Volume
	err = json.Unmarshal(*result.ApiResult, &volume)
	if err != nil {
		return fmt.Errorf("error deleting volume: %s,  %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully deleted volume %s", v.Name)

	return nil
}

func (v *Volume) updateAttributes(client *Client, attributesMap map[string]interface{}) (err error) {

	log.Debugf("Updating volume: %s", v.Name)

	if len(attributesMap) > 0 {
		url := fmt.Sprintf("api/rest/volumes/%d", v.ID)
		response, err := client.RestClient.R().SetBody(attributesMap).Put(url)

		result, err := CheckAPIResponse(response, err)
		if err != nil {
			return fmt.Errorf("error updating volume: %s,  %s", v.Name, err.Error())
		}

		err = json.Unmarshal(*result.ApiResult, &v)
		if err != nil {
			return fmt.Errorf("error updating volume: %s,  %s", v.Name, err.Error())
		}

		log.Infof("Succesfully updated volume %s", v.Name)
	}
	return nil
}

//UpdateName sets volume name
func (v *Volume) UpdateName(client *Client, name string) error {

	log.Debugf("Renaming volume %s", v.Name)

	body := map[string]interface{}{"name": name}
	err := v.updateAttributes(client, body)
	if err != nil {
		return fmt.Errorf("failed to rename volume %s, %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully renamed volume to %s", v.Name)

	return nil
}

//UpdateProvisioning sets volume thin/thick provision type
func (v *Volume) UpdateProvisioning(client *Client, provtype string) error {

	log.Debugf("Updating provisioning type for volume %s", v.Name)

	body := map[string]interface{}{"provtype": provtype}
	err := v.updateAttributes(client, body)
	if err != nil {
		return fmt.Errorf("failed to update provisioning type %s, %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully updated provisioning type to %s for volume %s", v.Provtype, v.Name)

	return nil
}

//UpdateSsdEnabled enable/disable volume SSD cache flag
func (v *Volume) UpdateSsdEnabled(client *Client, ssdEnabled bool) error {

	log.Debugf("Updating provisioning type for volume %s", v.Name)

	body := map[string]interface{}{"ssd_enabled": ssdEnabled}
	err := v.updateAttributes(client, body)
	if err != nil {
		return fmt.Errorf("failed to update ssd_enabled to %s, %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully updated ssd_enabled to %v for volume %s", v.SsdEnabled, v.Name)

	return nil
}

//UpdateWriteProtected volume parameter
func (v *Volume) UpdateWriteProtected(client *Client, writeProtected bool) error {

	log.Debugf("Updating write protected for volume %s", v.Name)

	body := map[string]interface{}{"write_protected": writeProtected}
	err := v.updateAttributes(client, body)
	if err != nil {
		return fmt.Errorf("failed to update write_protected to %s, %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully updated write_protected to %v for volume %s", v.WriteProtected, v.Name)

	return nil
}

//UpdateSize volume parameter
func (v *Volume) UpdateSize(client *Client, size uint64) error {

	log.Debugf("Updating provisioning type for volume %s", v.Name)

	body := map[string]interface{}{"size": size}
	err := v.updateAttributes(client, body)
	if err != nil {
		return fmt.Errorf("failed to update size to %s, %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully updated size to %v for volume %s", v.WriteProtected, v.Name)

	return nil
}

//Snapshot create volume snapshot
func (v *Volume) Snapshot(client *Client, name string) (snapshot *Volume, err error) {

	log.Debugf("Creating snapshot: %s", v.Name)

	url := "api/rest/volumes"
	body := map[string]interface{}{"parent_id": v.ID}

	if name == "" {
		body["name"] = fmt.Sprintf("auto-snapshot-%s", uuid.New())
	}
	response, err := client.RestClient.R().SetBody(body).Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf("error creating volume: %s,  %s", v.Name, err.Error())
	}

	err = json.Unmarshal(*result.ApiResult, &snapshot)
	if err != nil {
		return nil, fmt.Errorf("error creating volume: %s,  %s", v.Name, err.Error())
	}

	log.Debugf("Succesfully created snapshot %s for volume %s", snapshot.Name, v.Name)

	return snapshot, nil
}

//Restore volume from snapshot
func (v *Volume) Restore(client *Client, snapshotID uint64) (err error) {

	log.Debugf("Restoring volume %s from snapshot ID %d", v.Name, snapshotID)

	url := fmt.Sprintf("api/rest/volumes/%d/restore", v.ID)
	body := fmt.Sprintf("%d", snapshotID)

	response, err := client.RestClient.R().SetBody(body).SetQueryParam("approved", "true").Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf("error restoring volume: %s from snapshot ID %d,  %s", v.Name, snapshotID, err.Error())
	}

	var operationResult bool
	err = json.Unmarshal(*result.ApiResult, &operationResult)
	if err != nil {
		return fmt.Errorf("error restoring volume: %s from snapshot ID %d,  %s", v.Name, snapshotID, err.Error())
	}
	if !operationResult {
		return fmt.Errorf("error restoring volume: %s from snapshot ID %d, operation not completed successfully", v.Name, snapshotID)
	}

	log.Debugf("Succesfully restored volume %s to snapshotID %d", v.Name, snapshotID)

	return nil
}

//Refresh update snapshot from volume
func (v *Volume) Refresh(client *Client, snapshotID uint64) (err error) {

	log.Debugf("Refreshing volume %s to snapshot ID %d", v.Name, snapshotID)

	url := fmt.Sprintf("api/rest/volumes/%d/refresh", snapshotID)

	body := map[string]interface{}{}
	body["source_id"] = v.ID

	response, err := client.RestClient.R().SetBody(body).SetQueryParam("approved", "true").Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf("error refreshing volume: %s to snapshot ID %d,  %s", v.Name, snapshotID, err.Error())
	}

	var volume Volume
	err = json.Unmarshal(*result.ApiResult, &volume)
	if err != nil {
		return fmt.Errorf("error refreshing volume: %s to snapshot ID %d,  %s", v.Name, snapshotID, err.Error())
	}

	log.Debugf("Succesfully refreshed volume %s to snapshotID %d", v.Name, snapshotID)

	return nil
}
