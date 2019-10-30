package infinibox

import (
	"encoding/json"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
)

type Metadata struct {
	Key      string      `json:"key"`
	Value    interface{} `json:"value"`
	ID       int64       `json:"id"`
	ObjectID int64       `json:"object_id"`
}

func (c *Client) GetAllMetadata() (*[]Metadata, error) {

	log.Debugf("Getting all metadata")

	url := "api/rest/metadata/"
	response, err := c.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error getting all metadata, %s", err.Error()))
	}

	if num := result.ApiMetadata["number_of_objects"]; num == nil {
		return nil, errors.New("cannot parse metadata for number_of_objects field")
	} else {
		if num == float64(0) {
			log.Debugln("metadata is empty")
			return nil, nil
		}
	}

	var allMetadata []Metadata
	err = json.Unmarshal(*result.ApiResult, &allMetadata)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error getting all metadata, %s", err.Error()))
	}

	return &allMetadata, nil
}

func (c *Client) GetMetadataByObject(objectID int64) (*[]Metadata, error) {

	log.Debugf("Getting metadata by objectID %s", objectID)

	url := fmt.Sprintf("api/rest/metadata/%d", objectID)
	response, err := c.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Getting metadata by objectID %d, %s", objectID, err.Error()))
	}

	if num := result.ApiMetadata["number_of_objects"]; num == nil {
		return nil, errors.New("cannot parse metadata for number_of_objects field")
	} else {
		if num == float64(0) {
			log.Infof("metadata is empty")
			return nil, nil
		}
	}

	var objectMetadata []Metadata
	err = json.Unmarshal(*result.ApiResult, &objectMetadata)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Getting metadata by objectID %s, %s", objectID, err.Error()))
	}

	return &objectMetadata, nil
}

func (c *Client) GetMetadataByObjectAndKey(objectID int64, key string) (*Metadata, error) {

	log.Debugf("Getting metadata by objectID %s and key %s", objectID, key)

	url := fmt.Sprintf("api/rest/metadata/%d/%s", objectID, key)
	response, err := c.RestClient.R().Get(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, err
	}

	var objectMetadata Metadata
	err = json.Unmarshal(*result.ApiResult, &objectMetadata)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Getting metadata by objectID %d and key %s, %s", objectID, key, err.Error()))
	}

	return &objectMetadata, nil
}

func (c *Client) AddMetadata(metadata *Metadata) error {

	log.Debugf("Adding metadata for objectID %s", metadata.ObjectID)

	url := fmt.Sprintf("api/rest/metadata/%d", metadata.ObjectID)
	body := map[string]interface{}{metadata.Key: metadata.Value}
	response, err := c.RestClient.R().SetBody(body).Put(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return errors.New(fmt.Sprintf("Adding metadata for objectID %d failed, %s", metadata.ObjectID, err.Error()))
	}

	var objectMetadata []Metadata
	err = json.Unmarshal(*result.ApiResult, &objectMetadata)
	if err != nil {
		return errors.New(fmt.Sprintf("Adding metadata for objectID %d failed, %s", metadata.ObjectID, err.Error()))
	}

	log.Debugf("Added metadata: %v to objectID %s", metadata.Value, metadata.ObjectID)
	return nil
}

func (c *Client) DeleteMetadata(objectID int64) error {

	log.Debugf("Deleting metadata for objectID %d", objectID)

	url := fmt.Sprintf("api/rest/metadata/%d", objectID)
	response, err := c.RestClient.R().Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return errors.New(fmt.Sprintf("Deleting metadata for objectID %s failed, %s", objectID, err.Error()))
	}

	var objectMetadata []Metadata
	err = json.Unmarshal(*result.ApiResult, &objectMetadata)
	if err != nil {
		return errors.New(fmt.Sprintf("Deleting metadata for objectID %s failed, %s", objectID, err.Error()))
	}

	log.Debugf("Deleted metadata: for objectID %s", objectID)
	return nil
}

func (c *Client) DeleteMetadataByKey(objectID int64, key string) error {

	log.Debugf("Deleting metadata for objectID %s and key %s", objectID, key)

	url := fmt.Sprintf("api/rest/metadata/%d/%s", objectID, key)
	response, err := c.RestClient.R().Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return errors.New(fmt.Sprintf("Deleting metadata for objectID %s and key %s failed, %s", objectID, key, err.Error()))
	}

	var objectMetadata []Metadata
	err = json.Unmarshal(*result.ApiResult, &objectMetadata)
	if err != nil {
		return errors.New(fmt.Sprintf("Deleting metadata for objectID %s and key %s failed, %s", objectID, key, err.Error()))
	}

	log.Debugf("Deleted metadata: for objectID %s and key %s", objectID, key)
	return nil
}
