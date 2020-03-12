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

	db, err := sql.Open("mysql", "admin:xxx@tcp(xxxx:3306)/jupyterhub?charset=utf8")
	if err != nil {
		panic(err)

	}
	namespace := "kubeflow"
	var userMap map[string]string /*创建集合 */
	userMap = make(map[string]string)
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
			rows, err := db.Query("SELECT user_id, state, `name` FROM spawners WHERE server_id = ?", name)
			if err != nil {
				log.Fatal(err)
			}
			for rows.Next() {
				var state, user_id, spawner_name sql.NullString
				if err := rows.Scan(&user_id, &state, &spawner_name); err != nil {
					log.Fatal(err)
				}
				var i map[string]interface{}
				if err := json.Unmarshal([]byte(state.String), &i); err != nil {
					fmt.Println(state.String)
					fmt.Println("json parse error: ", err)
					continue
				}

				pod := i["pod_name"].(string)
				podEntity, err := clientset.CoreV1().Pods(namespace).Get(pod, metav1.GetOptions{})
				if errors.IsNotFound(err) {
					fmt.Printf("Pod %s in namespace %s not found\n", pod, namespace)
					user_name, ok :=  userMap[user_id.String]
					if (ok) {
						deleteHubSpawn(user_name, spawner_name.String)
					} else {
						userMap[user_id.String] = QueryUserName(db, user_id.String)
						deleteHubSpawn(userMap[user_id.String], spawner_name.String)
					}
				} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
					fmt.Printf("Error getting pod %s in namespace %s: %v\n",
						pod, namespace, statusError.ErrStatus.Message)
				} else if err != nil {
					log.Fatal(err)
				} else {
					status := podEntity.Status.Phase
					if status != "Running" {
						fmt.Printf("Found %s pod %s\n", status, pod)
						//fmt.Printf("Delete %s pod %s in namespace %s\n", status, pod, namespace)
						//if err := clientset.CoreV1().Pods(namespace).Delete(pod, &metav1.DeleteOptions{}); err != nil {
						//	log.Fatal(err)
						//}
						user_name, ok :=  userMap[user_id.String]
						if (ok) {
							deleteHubSpawn(user_name, spawner_name.String)
						} else {
							userMap[user_id.String] = QueryUserName(db, user_id.String)
							deleteHubSpawn(userMap[user_id.String], spawner_name.String)
						}
					}
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

func deleteHubSpawn(user string, server string) {
	url := "http://com/hub/api/users/"+user+"/servers/"+server
	payload := strings.NewReader("{\"remove\": true}")
	req, _ := http.NewRequest("DELETE", url, payload)
	fmt.Printf("Delete %s server %s.\n", user, server)
	req.Header.Add("Authorization", "token ")
	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(string(body))
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
