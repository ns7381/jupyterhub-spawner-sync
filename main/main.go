package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	//
	// Uncomment to load all auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth"
	//
	// Or uncomment to load specific auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth/azure"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/openstack"
)


type info struct {
	pod_name string `json:"pod_name"`
}

func main() {
	var kubeconfig *string
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	db, err := sql.Open("mysql", "admin:admin-:=@tcp(jupyterhub-mysqlha-write.kubeflow:3306)/jupyterhub?charset=utf8")
	if err != nil {
		panic(err)

	}
	namespace := "kubeflow"
	for {
		rows, err := db.Query("SELECT id FROM servers")
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s\n", name)
			rows, err := db.Query("SELECT state FROM spawners WHERE server_id = ?", name)
			if err != nil {
				log.Fatal(err)
			}
			for rows.Next() {
				var state string
				if err := rows.Scan(&state); err != nil {
					log.Fatal(err)
				}
				fmt.Printf("%s\n", state)
				var i info
				if err := json.Unmarshal([]byte(state), &i); err != nil {
					fmt.Println("ugh: ", err)
				}
				pod := i.pod_name
				_, err = clientset.CoreV1().Pods(namespace).Get(pod, metav1.GetOptions{})
				if errors.IsNotFound(err) {
					fmt.Printf("Pod %s in namespace %s not found\n", pod, namespace)
				} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
					fmt.Printf("Error getting pod %s in namespace %s: %v\n",
						pod, namespace, statusError.ErrStatus.Message)
				} else if err != nil {
					panic(err.Error())
				} else {
					fmt.Printf("Found pod %s in namespace %s\n", pod, namespace)
				}
			}
			if err := rows.Err(); err != nil {
				log.Fatal(err)
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
		time.Sleep(10 * time.Second)
	}
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
