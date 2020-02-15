package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/openfaas-incubator/faas-idler/types"

	providerTypes "github.com/openfaas/faas-provider/types"
	"github.com/openfaas/faas/gateway/metrics"
)

const scaleLabel = "com.openfaas.scale.zero"
const prometheusScrapeInterval = 15

var dryRun bool

var writeDebug bool

var functionTouches = make(map[string]float64)

type Credentials struct {
	Username string
	Password string
}

func main() {
	config, configErr := types.ReadConfig()
	if configErr != nil {
		log.Panic(configErr.Error())
		os.Exit(1)
	}

	flag.BoolVar(&dryRun, "dry-run", false, "use dry-run for scaling events")
	flag.Parse()

	if val, ok := os.LookupEnv("write_debug"); ok && (val == "1" || val == "true") {
		writeDebug = true
	}

	credentials := Credentials{}

	secretMountPath := "/var/secrets/"
	if val, ok := os.LookupEnv("secret_mount_path"); ok && len(val) > 0 {
		secretMountPath = val
	}

	if val, err := readFile(path.Join(secretMountPath, "basic-auth-user")); err == nil {
		credentials.Username = val
	} else {
		log.Printf("Unable to read username: %s", err)
	}

	if val, err := readFile(path.Join(secretMountPath, "basic-auth-password")); err == nil {
		credentials.Password = val
	} else {
		log.Printf("Unable to read password: %s", err)
	}

	client := &http.Client{}
	version, err := getVersion(client, config.GatewayURL, &credentials)

	if err != nil {
		panic(err)
	}

	log.Printf("Gateway version: %s, SHA: %s\n", version.Version.Release, version.Version.SHA)

	fmt.Printf(`dry_run: %t
gateway_url: %s
inactivity_duration: %s
reconcile_interval: %s
`, dryRun, config.GatewayURL, config.InactivityDuration, config.ReconcileInterval)

	if len(config.GatewayURL) == 0 {
		fmt.Println("gateway_url (faas-netes/faas-swarm) is required.")
		os.Exit(1)
	}

	for {
		fmt.Println("===== started =====")
		reconcile(client, config, &credentials)
		fmt.Println("ONE ROUND OVER ===================================== ")
		time.Sleep(config.ReconcileInterval)
	}
}

// Get RESTful get
func Get(url string) (int, []byte) {
	_res, err := http.Get(url)
	if err != nil {
		fmt.Printf("httpGet error\t%v", err)
	}
	defer _res.Body.Close()
	_bytes, err := ioutil.ReadAll(_res.Body)
	if err != nil {
		fmt.Printf("ReadAll error\t%v", err)
	}
	return _res.StatusCode, _bytes
}

func gatewayFunctionInvocationTotal(functionName string) float64 {
	// TODO: Parsing metrics
	// gateway_function_invocation_total{code="200",function_name="sethostsport"} 16

	_url := "http://gateway-metrics:8082/metrics"
	//	_url = "http://localhost:8082/metrics"
	//	fmt.Println(_url)

	_, _dataStr := Get(_url)
	//	fmt.Println(string(_dataStr))
	_data := strings.Split(string(_dataStr), "\n")

	var _sum int
	_sum = 0

	for _, row := range _data {
		//		fmt.Println(strings.HasPrefix(row, "gateway_function_invocation_total"))
		// gateway_function_invocation_total{code="200",function_name="sethostsport"} 16
		// skip the empty
		if row == "" {
			continue
		}

		// skip the lines not started with the target metrics
		if !strings.HasPrefix(row, "gateway_function_invocation_total") {
			continue
		}

		// skip the unmatched functions
		_match := "function_name=\"" + functionName + "\""
		if _pos := strings.Index(row, _match); _pos == -1 {
			continue
		}

		_segs := strings.Split(row, " ")
		_hits, err := strconv.Atoi(_segs[1])
		if err != nil {
			fmt.Println("Warn) failed to parse, skip:", _segs[1])
			continue
		}
		// fmt.Println(">", _segs[1], _hits)

		_sum += _hits

		//		fmt.Println(">", row, "<", strings.HasPrefix(row, "gateway_function_invocation_total"))
	}
	return float64(_sum)
}

func readFile(path string) (string, error) {
	if _, err := os.Stat(path); err == nil {
		data, readErr := ioutil.ReadFile(path)
		return strings.TrimSpace(string(data)), readErr
	}
	return "", nil
}

func buildMetricsMap(client *http.Client, functions []providerTypes.FunctionStatus, config types.Config) map[string]float64 {
	query := metrics.NewPrometheusQuery(config.PrometheusHost, config.PrometheusPort, client)
	metrics := make(map[string]float64)

	duration := fmt.Sprintf("%dm", int(config.InactivityDuration.Minutes()))
	// duration := "5m"

	for _, function := range functions {
		querySt := url.QueryEscape(`sum(rate(gateway_function_invocation_total{function_name="` + function.Name + `", code=~".*"}[` + duration + `])) by (code, function_name)`)

		fmt.Printf(querySt)
		res, err := query.Fetch(querySt)
		if err != nil {
			log.Println(err)
			continue
		}

		if len(res.Data.Result) > 0 || function.InvocationCount == 0 {

			if _, exists := metrics[function.Name]; !exists {
				metrics[function.Name] = 0
			}

			for _, v := range res.Data.Result {

				if writeDebug {
					fmt.Println(v)
				}

				if v.Metric.FunctionName == function.Name {
					metricValue := v.Value[1]
					switch metricValue.(type) {
					case string:

						f, strconvErr := strconv.ParseFloat(metricValue.(string), 64)
						if strconvErr != nil {
							log.Printf("Unable to convert value for metric: %s\n", strconvErr)
							continue
						}

						metrics[function.Name] = metrics[function.Name] + f
					}
				}
			}
		}
	}
	return metrics
}

func reconcile(client *http.Client, config types.Config, credentials *Credentials) {
	functions, err := queryFunctions(client, config.GatewayURL, credentials)

	if err != nil {
		log.Println("Warn)", err)
		return
	}
	fmt.Println("Debug)", "function list fetched")

	for _, function := range functions {

		fmt.Printf("Info) %v\n", function)

		go func(client *http.Client, function providerTypes.FunctionStatus, config types.Config, credentials *Credentials) {

			// Criteria 1: skip thouse no lables
			if function.Labels != nil {
				labels := *function.Labels
				labelValue := labels[scaleLabel]

				if labelValue != "1" && labelValue != "true" {
					if writeDebug {
						log.Printf("Skip: %s due to missing label\n", function.Name)
					}
					fmt.Printf("Info) %s is not labeled, skip the pod...\n", function.Name)
					return
				}
			}

			// generate initial map
			if _, ok := functionTouches[function.Name]; !ok {
				functionTouches[function.Name] = -1
			}
			fmt.Printf("retvalCache\t%s\t%f\n", function.Name, functionTouches[function.Name])

			if val, _ := getReplicas(client, config.GatewayURL, function.Name, credentials); val != nil && val.AvailableReplicas > 0 {
				retvalBefore := 0.0
				retvalAfter := 0.0

				retvalBefore = gatewayFunctionInvocationTotal(function.Name)
				fmt.Printf("1st check\t%s\t%f\t%f\n", function.Name, functionTouches[function.Name], retvalBefore)
				// fmt.Printf("retvalBefore\tfunction.InvocationCount\t%f\t%f\n", retvalBefore, function.InvocationCount)
				time.Sleep(config.InactivityDuration)
				retvalAfter = gatewayFunctionInvocationTotal(function.Name)
				fmt.Printf("2nd check\t%s\t%f\t%f\t%f\n", function.Name, functionTouches[function.Name], retvalBefore, retvalAfter)
				// fmt.Printf("retvalAfter\tfunction.InvocationCount\t%f\t%f\n", retvalAfter, function.InvocationCount)
				if retvalAfter == retvalBefore && retvalAfter == functionTouches[function.Name] {
					// Idles InactivityDuration, scales to zero
					fmt.Printf("**** SCALE TO ZERO *****\t%v\tvalMap\tretvalBefore\tretvalAfter\t%v\t%v\t%v\n", function.Name, functionTouches[function.Name], retvalBefore, retvalAfter)
					sendScaleEvent(client, config.GatewayURL, function.Name, uint64(0), credentials)
				}
			}

			functionTouches[function.Name] = gatewayFunctionInvocationTotal(function.Name)

		}(client, function, config, credentials)
	}
}

func getReplicas(client *http.Client, gatewayURL string, name string, credentials *Credentials) (*providerTypes.FunctionStatus, error) {
	item := &providerTypes.FunctionStatus{}
	var err error

	req, _ := http.NewRequest(http.MethodGet, gatewayURL+"system/function/"+name, nil)
	req.SetBasicAuth(credentials.Username, credentials.Password)

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	bytesOut, _ := ioutil.ReadAll(res.Body)

	err = json.Unmarshal(bytesOut, &item)

	return item, err
}

func queryFunctions(client *http.Client, gatewayURL string, credentials *Credentials) ([]providerTypes.FunctionStatus, error) {
	list := []providerTypes.FunctionStatus{}
	var err error

	req, _ := http.NewRequest(http.MethodGet, gatewayURL+"system/functions", nil)
	req.SetBasicAuth(credentials.Username, credentials.Password)

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	bytesOut, _ := ioutil.ReadAll(res.Body)

	err = json.Unmarshal(bytesOut, &list)

	return list, err
}

func sendScaleEvent(client *http.Client, gatewayURL string, name string, replicas uint64, credentials *Credentials) {
	if dryRun {
		fmt.Printf("dry-run: Scaling %s to %d replicas\n", name, replicas)
		return
	}

	scaleReq := providerTypes.ScaleServiceRequest{
		ServiceName: name,
		Replicas:    replicas,
	}

	var err error

	bodyBytes, _ := json.Marshal(scaleReq)
	bodyReader := bytes.NewReader(bodyBytes)

	req, _ := http.NewRequest(http.MethodPost, gatewayURL+"system/scale-function/"+name, bodyReader)
	req.SetBasicAuth(credentials.Username, credentials.Password)

	res, err := client.Do(req)

	if err != nil {
		log.Println(err)
		return
	}
	log.Println("Scale", name, res.StatusCode, replicas)

	if res.Body != nil {
		defer res.Body.Close()
	}
}

// Version holds the GitHub Release and SHA
type Version struct {
	Version struct {
		Release string `json:"release"`
		SHA     string `json:"sha"`
	}
}

func getVersion(client *http.Client, gatewayURL string, credentials *Credentials) (Version, error) {
	version := Version{}
	var err error

	req, _ := http.NewRequest(http.MethodGet, gatewayURL+"system/info", nil)
	req.SetBasicAuth(credentials.Username, credentials.Password)

	res, err := client.Do(req)
	if err != nil {
		return version, err
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	bytesOut, _ := ioutil.ReadAll(res.Body)

	err = json.Unmarshal(bytesOut, &version)

	return version, err
}
