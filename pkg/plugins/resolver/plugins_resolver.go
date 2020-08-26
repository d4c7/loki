// +build ignore

package main

import (
	"bufio"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
	"time"
)

const (
	pluginsEnginePath = "."
	pluginsFile       = "../../plugins"
	pluginsRepoPath   = "./repo"
)

type Plugin struct {
	Name       string
	ImportPath string
}

func main() {
	log.Println("resolve plugins")
	var plugins []Plugin

	sourceFile, err := filepath.Abs(pluginsFile)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open(sourceFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal(err)
		}
		log.Println("plugins file not found")
	} else {
		defer file.Close()
		log.Println("process plugins file")

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {

			repo := scanner.Text()

			if ok, _ := regexp.Match(`\s*#`, []byte(repo)); ok {
				continue
			}

			log.Println("processing " + repo)

			_, name := fileParts(repo)
			//if target exists
			target := pluginsRepoPath + "/" + name

			info, err := os.Stat(target)
			if os.IsNotExist(err) {
				log.Println("clone repo " + repo)

				cmd := exec.Command("git", "clone", repo, target)
				cmd.Stdout = os.Stdout
				err := cmd.Run()
				if err != nil {
					log.Fatal(err)
				}
			} else {
				if !info.IsDir() {
					log.Fatal(errors.New(target + " is not dir"))
				} else {
					log.Println("do not override local " + target)
				}
			}
		}

		log.Println("resolve plugins from repositories ")

		err = filepath.Walk(pluginsRepoPath,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if !strings.HasSuffix(path, ".go") {
					return nil
				}

				n, err := ioutil.ReadFile(path)
				if err != nil {
					panic(err)
				}
				nf := strings.ReplaceAll(string(n), "\n", " ")
				packs := regexp.MustCompile(`package\s+(\S+)`).FindStringSubmatch(nf)
				if len(packs) > 1 {
					pack := packs[1]

					// func Descriptor() stages.PluginDescriptor
					if regexp.MustCompile(`func\s+Descriptor\s*\(\s*\)\s+stages\.PluginDescriptor`).Match([]byte(nf)) {
						log.Printf("pluging %s found at %s ", pack, path)
						p, _ := fileParts(path)
						plugins = append(plugins, Plugin{Name: pack, ImportPath: "github.com/grafana/loki/pkg/plugins/" + p})
					}
				}
				return nil
			})
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Println("generate resolved go file ")

	templ, err := template.ParseFiles(pluginsEnginePath + "/resolver/resolved.template")
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(pluginsEnginePath + "/resolved.go")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	err = templ.Execute(f, struct {
		Timestamp time.Time
		Plugins   []Plugin
		Source    string
	}{
		Timestamp: time.Now(),
		Plugins:   plugins,
		Source:    sourceFile,
	})

	if err != nil {
		log.Fatal(err)
	}

}

func fileParts(name string) (string, string) {
	i := strings.LastIndex(name, "/")
	if i < 0 {
		return "", name
	}
	return name[:i], name[i+1:]
}
