/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"github.com/kwstars/redis-dump/internal/myredis"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "迁移redis数据库",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:
Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	ValidArgs: []string{"source", "dest"},
	Args: func(cmd *cobra.Command, args []string) error {
		if !cmd.Flag("source").Changed && !cmd.Flag("dest").Changed {
			return cmd.Usage()
		}

		for _, c := range []string{"source", "dest"} {
			flag := cmd.Flag(c)
			var command string
			if flag.Changed {
				command = flag.Value.String()
			} else {
				command = flag.DefValue
			}

			command = strings.Join([]string{"redis://", command}, "")

			if _, err := url.Parse(command); err != nil {
				return cmd.Usage()
			}
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		sIP, sPort, sPassword, sDB, err := getParameter(cmd.Flag("source"))
		if err != nil {
			log.Printf("%+v\n", err)
			return
		}
		dIP, dPort, dPassword, dDB, err := getParameter(cmd.Flag("dest"))
		if err != nil {
			log.Printf("%+v\n", err)
			return
		}

		sRedis, cf1, err1 := myredis.NewRedis(sIP, sPort, sPassword, sDB)
		if err1 != nil {
			log.Println(err1)
			return
		}
		defer cf1()
		dRedis, cf2, err2 := myredis.NewRedis(dIP, dPort, dPassword, dDB)
		if err2 != nil {
			log.Println(err2)
			return
		}
		defer cf2()

		var wg sync.WaitGroup
		sem := make(chan struct{}, 20)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for v := range sRedis.Scan(ctx, "*", 1000) {
			if v.Err != nil {
				log.Printf("%+v", v.Err)
				return
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(d []string) {
				defer wg.Done()
				defer func() { <-sem }()
				data, err := sRedis.DUMP(d)
				if err != nil {
					log.Printf("%+v\n", err)
					return
				}
				if err := dRedis.RESTORE(data); err != nil {
					log.Printf("%v\n", err)
				}
			}(v.Data)
		}
		wg.Wait()
		close(sem)
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
	migrateCmd.Flags().SortFlags = false
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// migrateCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	//migrateCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	migrateCmd.PersistentFlags().String("source", "", "[:password@]host[:port][/database] (required)")
	migrateCmd.PersistentFlags().String("dest", "", "[:password@]host[:port][/database] (required)")
	migrateCmd.MarkPersistentFlagRequired("source")
	migrateCmd.MarkPersistentFlagRequired("dest")
}

func getParameter(flag *pflag.Flag) (ip, port, password string, db int, err error) {
	var command string
	if flag.Changed {
		command = flag.Value.String()
	} else {
		command = flag.DefValue
	}
	command = strings.Join([]string{"redis://", command}, "")

	url, _ := url.Parse(command)

	ip, port, err = net.SplitHostPort(url.Host)
	if err != nil {
		return "", "", "", 0, errors.Errorf("splitHostPort: %v", err)
	}

	password, _ = url.User.Password()

	dbString := strings.TrimPrefix(url.Path, "/")
	db, err = strconv.Atoi(dbString)
	if err != nil {
		return "", "", "", 0, errors.Errorf("TrimPrefix: %v", err)
	}

	return
}
