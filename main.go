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
		reconcile(client, config, &credentials)
		time.Sleep(config.ReconcileInterval)
		fmt.Printf("\n")
	}
}

/*
 todie
func testGateway() {
	curlStr := fmt.Sprintf("http://gateway-metrics:8082/metrics")
	// curlStr := fmt.Sprintf("http://elasticsearch.marvin:9200")
	_res, _ := http.Get(curlStr)
	defer _res.Body.Close()
	_bytes, _ := ioutil.ReadAll(_res.Body)

	fmt.Printf("responseData: %v\n", string(_bytes))

	// TODO: Parsing metrics
	// gateway_function_invocation_total{code="200",function_name="sethostsport"} 16

}
*/

// restful Get
func Get(url string) (int, []byte) {
	_res, _ := http.Get(url)
	defer _res.Body.Close()
	_bytes, _ := ioutil.ReadAll(_res.Body)
	return _res.StatusCode, _bytes
}

func testGateway(functionName string) int {
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
			fmt.Println("failed to parse, skip:", _segs[1])
			continue
		}
		// fmt.Println(">", _segs[1], _hits)

		_sum += _hits

		fmt.Println(">", row, "<", strings.HasPrefix(row, "gateway_function_invocation_total"))
	}
	return _sum
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

func scaleCriteria(client *http.Client, function providerTypes.FunctionStatus, config types.Config, credentials *Credentials) float64 {
	// skip those w/o the label "com.openfaas.scale.zero=true"
	if function.Labels != nil {
		labels := *function.Labels
		labelValue := labels[scaleLabel]

		if labelValue != "1" && labelValue != "true" {
			if writeDebug {
				log.Printf("Skip: %s due to missing label\n", function.Name)
			}
			fmt.Println("Not labeled, skip the pod...")
			return 1
		}
	}

	// w/ labels start from here
	query := metrics.NewPrometheusQuery(config.PrometheusHost, config.PrometheusPort, client)
	duration := fmt.Sprintf("%dm", int(config.InactivityDuration.Minutes()))
	ivCount := 0.0
	requirement1 := false
	requirement2 := false

	query1 := url.QueryEscape(`gateway_function_invocation_total{function_name="` + function.Name + `", code=~".*"}`)
	res, err := query.Fetch(query1)
	if err != nil {
		log.Println(err)
		return ivCount
	}
	fmt.Printf("query1: %v\n", res.Data.Result)

	if len(res.Data.Result) > 0 {
		invocationValue := res.Data.Result[0].Value[1]
		fmt.Printf("invocationValue: %v\n", invocationValue)
		switch invocationValue.(type) {
		case string:
			invocationCount, parseErr := strconv.ParseFloat(invocationValue.(string), 64)
			if parseErr != nil {
				log.Printf("parseErr\t%v\n", parseErr)
			}
			if invocationCount == 1 {
				requirement1 = true
			}
		}
	}
	query2 := url.QueryEscape(`count_over_time(gateway_function_invocation_total{function_name="` + function.Name + `", code=~".*"}[` + duration + `])`)

	res, err = query.Fetch(query2)
	if err != nil {
		log.Println(err)
		return ivCount
	}
	fmt.Printf("query2: %v\n", res.Data.Result)

	if len(res.Data.Result) > 0 {
		scrapeOverTimeValue := res.Data.Result[0].Value[1]
		fmt.Printf("countOverTimeValue: %v\n", scrapeOverTimeValue)
		switch scrapeOverTimeValue.(type) {
		case string:
			scrapeOverTime, parseErr := strconv.ParseFloat(scrapeOverTimeValue.(string), 64)
			if parseErr != nil {
				log.Printf("parseErr\t%v\n", parseErr)
			}
			// 15 seconds is the scrape interval from Prometheus configuration
			criteria := config.InactivityDuration.Minutes() * 60 / prometheusScrapeInterval
			fmt.Printf("criteria: %v\n", criteria)
			if scrapeOverTime < criteria {
				fmt.Println("First time spawned, don't kill it!")
				return 1 // > 0: don't terminate it!
			} else if scrapeOverTime == criteria {
				requirement2 = true
			}
		}
	}

	// First spawned and idle for three minutes. can be scaled to zero
	if requirement1 && requirement2 {
		fmt.Println("First time spawned but idled 3 minutes, kill it!")
		return 0
	}

	// Normal case
	fmt.Println("Normal cases going on ... ")
	querySt := url.QueryEscape(`sum(rate(gateway_function_invocation_total{function_name="` + function.Name + `", code=~".*"}[` + duration + `])) by (code, function_name)`)

	res, err = query.Fetch(querySt)
	if err != nil {
		log.Println(err)
		return ivCount
	}

	if len(res.Data.Result) > 0 || function.InvocationCount == 0 {
		// fmt.Printf("function.Name\t%v\n", function.Name)
		fmt.Printf("res.Data.Result: %v\n", res.Data.Result)
		fmt.Printf("InvocationCount: %v\n", function.InvocationCount)

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
					fmt.Printf("f\t%v\n", f)
					ivCount += f
				}
			}
		}
	}

	// fmt.Printf("ivCount: %v\n", ivCount)

	return ivCount
}

func reconcile(client *http.Client, config types.Config, credentials *Credentials) {
	functions, err := queryFunctions(client, config.GatewayURL, credentials)

	if err != nil {
		log.Println(err)
		return
	}

	// double confirm for the sake of 15 second scrape buffering
	for _, function := range functions {

		// TODO:
		// retvalBefore := testGateway()

		go func(client *http.Client, function providerTypes.FunctionStatus, config types.Config, credentials *Credentials) {
			// fmt.Println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
			// time.Sleep(time.Second * 3)

			// TODO: directly check gateway metrics from CURL

			// curl 10.43.225.85:8082/metrics | grep gateway_function_invocation_total | grep balance
			retvalBefore := testGateway(function.Name)
			fmt.Println ("Debug)", function.Name, retvalBefore )

			realScale := 0
			for i := 0; i < 2; i++ {
				// fmt.Println("------------------------------------------------------------------------------------------")
				// fmt.Println("")
				// fmt.Println("")
				// fmt.Println("")
				// fmt.Printf("Next function\t%v\n", function.Name)
				// fmt.Println("**************** start point ", i)
				// time.Sleep(time.Second * 5)
				firstCheck := scaleCriteria(client, function, config, credentials)

				// fmt.Println("------------------------\tFIRST round check\t", firstCheck)
				// fmt.Println("**************** middle point ", i)
				// fmt.Println("sleep 22 seconds .....")

				time.Sleep(time.Second * prometheusScrapeInterval)

				secondCheck := scaleCriteria(client, function, config, credentials)
				// fmt.Println("------------------------\tSECOND round check\t", secondCheck)

				replicaSize, _ := getReplicas(client, config.GatewayURL, function.Name, credentials)
				if firstCheck == float64(0) && secondCheck == float64(0) && replicaSize.AvailableReplicas > 0 {

					realScale++
					// time.Sleep(time.Second * 2)
					fmt.Printf("realScale++: %v\t%v\n", realScale, function.Name)

				}

				// fmt.Printf("realScale: %v\n", realScale)
				// fmt.Println("**************** end point ", i)
				time.Sleep(time.Second * prometheusScrapeInterval)

			}

			if realScale == 2 {
				if val, _ := getReplicas(client, config.GatewayURL, function.Name, credentials); val != nil && val.AvailableReplicas > 0 {
					fmt.Printf("realScale: %v\n", realScale)
					fmt.Printf("SCALE\t%s\tTO ZERO ...\n", function.Name)

					// TODO:
					retval := testGateway(function.Name)
					fmt.Println ("Debug)", function.Name, retvalBefore, " => ", retval )

					if retval == retvalBefore {
						sendScaleEvent(client, config.GatewayURL, function.Name, uint64(0), credentials)
						fmt.Println ("Info)", function.Name, "scale to 0 due to ", retvalBefore, " = ", retval)
					}

//					sendScaleEvent(client, config.GatewayURL, function.Name, uint64(0), credentials)
				} else {
					fmt.Println("Info)", "IGNORE because replicas is 0 -------------------")
				}
			}

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
