package infinibox

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty"
	log "github.com/sirupsen/logrus"
	"net/url"
	"time"
)

//Config reprents client configuration struct
type Config struct {
	Username string
	Password string
	URL      string
	tenant   string
	Debug    bool
}

//APIError represents IBOX API response error struct
type APIError struct {
	Code     string
	Message  string
	Reasons  []interface{}
	Severity string
	IsRemote bool
	Data     interface{}
}

//APIMetadata represents IBOX API response metadata struct
type APIMetadata struct {
	Ready           bool `json:"ready"`
	Page            int  `json:"page,omitempty"`
	NumberOfObjects int  `json:"numberofobjects,omitempty"`
	PageSize        int  `json:"pagesize,omitempty"`
	PagesTotal      int  `json:"pagestotal,omitempty"`
}

//APIResponse represents IBOX API response composite struct
type APIResponse struct {
	APIError    map[string]interface{} `json:"error"`
	APIMetadata map[string]interface{} `json:"metadata"`
	APIResult   *json.RawMessage       `json:"result"`
}

//Client represents client struct
type Client struct {
	RestClient *resty.Client
	config     *Config
}

//NewClient function generates new client instance
func NewClient(config *Config) (*Client, error) {
	restClient, err := restyBasicClient(config)
	if err != nil {
		return nil, err
	}
	if restClient == nil {
		return nil, err
	}
	c := &Client{RestClient: restClient, config: config}
	return c, nil
}

func restyBasicClient(config *Config) (*resty.Client, error) {

	restclient := resty.New()
	restclient.SetHeaders(map[string]string{
		"Content-Type": "application/json",
		"User-Agent":   "go-client",
	})

	restclient.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	restclient.SetHostURL(config.URL)
	restclient.SetDisableWarn(true)
	if config.Debug {
		restclient.Debug = config.Debug
	}

	_, err := url.Parse(config.URL)
	if err != nil {

		return nil, err
	}

	log.Debug("Succesfully initialized infinibox client")

	return restclient, nil
}

//Login provides client login method
func (c *Client) Login() error {

	log.Debug("Logging into infinibox")

	url := "api/rest/users/login"
	body := map[string]string{"username": c.config.Username, "password": c.config.Password}

	response, err := c.RestClient.R().SetBody(body).Post(url)
	_, err = CheckAPIResponse(response, err)
	if err != nil {
		return err
	}

	c.RestClient.SetTimeout(time.Duration(5 * time.Second))
	c.RestClient.SetRetryCount(3)

	log.Debug("Logged-in succesfully")

	return nil
}

//SetTenant client method sets tenant id for provided tenant
func (c *Client) SetTenant(tenantname string) error {

	log.Debugf("Setting tenant: %s", tenantname)

	var tenant *Tenant

	tenant, err := c.GetTenantByName(tenantname)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	if tenant != nil {
		log.Debugf("Setting tenant id to: %d", tenant.ID)
		c.config.tenant = fmt.Sprintf("%d", tenant.ID)
	}
	return nil

}

//CheckAPIResponse parses API response for error and result
func CheckAPIResponse(res *resty.Response, err error) (apiresponse *APIResponse, er error) {
	defer func() {
		if recovered := recover(); recovered != nil && er == nil {
			er = fmt.Errorf("panic occured while parsing management api response " + fmt.Sprint(recovered) + "for request " + res.Request.URL)
		}
	}()

	if err != nil {
		return nil, err
	}

	if res.StatusCode() == 500 {
		return nil, fmt.Errorf(res.Status())
	}

	if er := json.Unmarshal(res.Body(), &apiresponse); er != nil {
		log.Error("error unmarshalling response body to API RESPONSE type")
		return nil, er
	}

	if apiresponse.APIError != nil {
		code := ""
		message := ""

		if _, ok := apiresponse.APIError["code"].(string); ok {
			code = apiresponse.APIError["code"].(string)
		}
		if _, ok := apiresponse.APIError["message"].(string); ok {
			message = apiresponse.APIError["message"].(string)
		}
		return nil, fmt.Errorf("{API ERRROR CODE: %s}, {API ERROR MESSAGE: %s}", code, message)
	}

	return apiresponse, nil

}
