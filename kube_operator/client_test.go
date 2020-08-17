package kube_operator

import (
	"encoding/base64"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"reflect"
	"testing"
)

const yamlStr = `apiVersion: charts.helm.k8s.io/v1alpha1
kind: ACustomResource
metadata:
  name: example-acustomresource
spec:
  affinity: {}
  fullnameOverride: ""
  image:
    digest: sha256:718ecb5dab5cfffd0bb3c63e85b59e608c4b2866eab8b3f6e3054d407b52b92f
    name: openhorizon/acustomresource
    pullPolicy: IfNotPresent
  imagePullSecrets: []
  ingress:
    annotations: {}
    enabled: false
    hosts:
    - host: chart-example.local
      paths: []
    tls: []
  nameOverride: ""
  nodeSelector: {}
  podSecurityContext: {}
  replicaCount: 1
  resources: {}
  securityContext: {}
  service:
    port: 1234
    topologyKey: '*'
    type: ClusterIP
  serviceAccount:
    annotations: {}
    create: true
    name: null
  tolerations: []
  `

func Test_makeAllKeysStrings(t *testing.T) {
	_, yamlFile, _ := getK8sObjectFromYaml([]YamlFile{YamlFile{Body: yamlStr}}, nil)

	cr := make(map[string]interface{})
	yaml.UnmarshalStrict([]byte(yamlFile[0].Body), &cr)
	yamlMap := makeAllKeysStrings(cr)

	if nonStringMap := findNonStringKey(yamlMap); nonStringMap != nil {
		t.Errorf("Non-string type  %v", nonStringMap)
	}

}

func NewKubeClientFromConfig() KubeClient {
	config, err := clientcmd.BuildConfigFromFlags("", "/home/max/.microk8s/config")
	if err != nil {
		panic(err.Error())
	}
	clientset, _ := kubernetes.NewForConfig(config)
	return KubeClient{Client: clientset}
}

func Test_Install(t *testing.T) {
	opFiles, err := ioutil.ReadFile("/home/max/feijoa/helm-hw/operator-mod.tar.gz")
	if err != nil {
		t.Errorf("Error reading operator files: %v", err)
		return
	}

	client := NewKubeClientFromConfig()

	opData := base64.StdEncoding.EncodeToString(opFiles)
	if err != nil {
		t.Errorf("Error decoding operator files: %v", err)
		return
	}

	err = client.Uninstall(string(opData), "12345")
	err = client.Install(string(opData), map[string]string{"var1": "test-val"}, "12345")

	if err != nil {
		t.Errorf("Error installing operator: %v", err)
	}
}

func findNonStringKey(yamlMap interface{}) interface{} {
	if reflect.ValueOf(yamlMap).Kind() == reflect.Map {
		if stringKeyMap, ok := yamlMap.(map[string]interface{}); ok {
			for _, val := range stringKeyMap {
				if nonStringKey := findNonStringKey(val); nonStringKey != nil {
					return nonStringKey
				}
			}
		} else {
			return yamlMap
		}
	} else if reflect.ValueOf(yamlMap).Kind() == reflect.Slice {
		for _, elem := range yamlMap.([]interface{}) {
			if bad := findNonStringKey(elem); bad != nil {
				return bad
			}
		}
	}

	return nil
}
