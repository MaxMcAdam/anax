package kube_operator

import (
	"encoding/base64"
	"io/ioutil"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"testing"
)

func NewKubeClientFromConfig() KubeClient {
	config, err := clientcmd.BuildConfigFromFlags("", "/home/max/.microk8s/config")
	if err != nil {
		panic(err.Error())
	}
	clientset, _ := kubernetes.NewForConfig(config)
	return KubeClient{Client: clientset}
}

func Test_Install(t *testing.T) {
	opFiles, err := ioutil.ReadFile("./operator.tar.gz")
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
