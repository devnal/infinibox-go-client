package infinibox

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type Tenant struct {
	Capacity struct {
		TotalPhysicalCapacity  int64 `json:"total_physical_capacity"`
		AllocatedPhysicalSpace int64 `json:"allocated_physical_space"`
		TotalVirtualCapacity   int64 `json:"total_virtual_capacity"`
		AllocatedVirtualSpace  int64 `json:"allocated_virtual_space"`
	} `json:"capacity"`
	ID                int64  `json:"id"`
	Name              string `json:"name"`
	ShortTenantKey    int    `json:"short_tenant_key"`
	VisibleToSysadmin bool   `json:"visible_to_sysadmin"`
	CreatedAt         int64  `json:"created_at"`
	UpdatedAt         int64  `json:"updated_at"`
	EntityCounts      struct {
		Plugins       int64 `json:"plugins"`
		Pools         int64 `json:"pools"`
		NetworkSpaces int64 `json:"network_spaces"`
		Hosts         int64 `json:"hosts"`
		Clusters      int64 `json:"clusters"`
	} `json:"entity_counts"`
}

func (c *Client) GetTenantByName(tenantname string) (*Tenant, error) {

	queryRes, err := c.Find("tenants", "name", "eq", tenantname)

	if err != nil {
		return nil, err
	}

	var tenants []Tenant

	err = json.Unmarshal(*queryRes, &tenants)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("unable to decode tenants collection: %s query result, error: %s", tenantname, err.Error()))
	}

	if len(tenants) == 0 {
		return nil, fmt.Errorf(fmt.Sprintf("tenant %s not found", tenantname))
	}

	log.Debugf("Found tenant %#v", &tenants[0])

	return &tenants[0], nil
}

func (t *Tenant) Create(client *Client) (err error) {

	log.Debugf("Creating tenant: %s", t.Name)
	url := "api/rest/tenants"
	response, err := client.RestClient.R().SetBody(map[string]interface{}{
		"name": t.Name}).Post(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error creating tenant: %s,  %v", t.Name, err))
	}

	err = json.Unmarshal(*result.APIResult, &t)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("error creating tenant: %s,  %v", t.Name, err))
	}

	log.Debugf("Successfully created tenant %s", t.Name)

	return nil
}

func (t *Tenant) Delete(client *Client) (tenant *Tenant, err error) {

	log.Debugf("Deleting tenant: %s", t.Name)
	url := fmt.Sprintf("api/rest/tenants/%d", t.ID)
	response, err := client.RestClient.R().SetQueryParam("approved", "true").Delete(url)

	result, err := CheckAPIResponse(response, err)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error deleting tenant: %s,  %v", t.Name, err))
	}

	err = json.Unmarshal(*result.APIResult, &tenant)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("error deleting tenant: %s,  %v", t.Name, err))
	}

	log.Debugf("Successfully deleted tenant %s", t.Name)

	return tenant, nil
}

func (t *Tenant) updateAttributes(client *Client, attributesMap map[string]interface{}) (err error) {

	log.Debugf("Updating tenant: %s", t.Name)
	url := fmt.Sprintf("api/rest/tenants/%d", t.ID)

	if len(attributesMap) > 0 {
		response, err := client.RestClient.R().SetBody(attributesMap).Put(url)

		result, err := CheckAPIResponse(response, err)
		if err != nil {
			return fmt.Errorf(fmt.Sprintf("error updating tenant: %s,  %v", t.Name, err))
		}

		err = json.Unmarshal(*result.APIResult, &t)
		if err != nil {
			return fmt.Errorf(fmt.Sprintf("error updating tenant: %s,  %v", t.Name, err))
		}
	}

	log.Debugf("Successfully updated tenant %s", t.Name)

	return nil
}

func (t *Tenant) UpdateName(client *Client, name string) error {

	log.Debugf("Renaming tenant %s", t.Name)

	attributesMap := map[string]interface{}{"name": name}
	err := t.updateAttributes(client, attributesMap)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("failed to rename tenant %s, %s", t.Name, err.Error()))
	}

	log.Debugf("Succesfully renamed tenant %s to %s", t.Name, name)

	return nil
}
