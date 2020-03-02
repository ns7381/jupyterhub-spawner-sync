package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
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

	db, err := sql.Open("mysql", "admin:admin@tcp(jupyterhub-mysqlha-write.kubeflow:3306)/jupyterhub?charset=utf8")
	if err != nil {
		panic(err)

	}
	namespace := "kubeflow"
	for {
		fmt.Printf("run.....\n")
		rows, err := db.Query("SELECT id FROM servers")
		if err != nil {
			log.Fatal(err)
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				log.Fatal(err)
			}
			rows, err := db.Query("SELECT user_id, state, `name` FROM spawners WHERE server_id = ?", name)
			if err != nil {
				log.Fatal(err)
			}
			for rows.Next() {
				var state, user_id, spawner_name string
				if err := rows.Scan(&user_id, &state, &spawner_name); err != nil {
					log.Fatal(err)
				}
				var i map[string]interface{}
				if err := json.Unmarshal([]byte(state), &i); err != nil {
					fmt.Println("ugh: ", err)
				}
				pod := i["pod_name"].(string)
				_, err = clientset.CoreV1().Pods(namespace).Get(pod, metav1.GetOptions{})
				if errors.IsNotFound(err) {
					fmt.Printf("Pod %s in namespace %s not found\n", pod, namespace)
					deleteHubSpawn(QueryUserName(db, user_id), spawner_name)
				} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
					fmt.Printf("Error getting pod %s in namespace %s: %v\n",
						pod, namespace, statusError.ErrStatus.Message)
				} else if err != nil {
					log.Fatal(err)
				} else {
					//fmt.Printf("Found pod %s in namespace %s\n", pod, namespace)
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

func QueryUserName(DB *sql.DB, id string) string {
	var user string   //用new()函数初始化一个结构体对象
	row := DB.QueryRow("select `name` from users where id=?", id)
	//row.scan中的字段必须是按照数据库存入字段的顺序，否则报错
	if err := row.Scan(&user); err != nil {
		fmt.Printf("scan failed, err:%v\n", err)
	}
	return user
}

func deleteHubSpawn(user string, server string) string {
	url := "http://service/hub/api/users/"+user+"/server/"+server
	payload := strings.NewReader("{\"delete\": true}")
	req, _ := http.NewRequest("DELETE", url, payload)
	fmt.Printf("Delete %s server %s.\n", user, server)
	req.Header.Add("Authorization", "token xxx")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(body)
	return string(body)
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
