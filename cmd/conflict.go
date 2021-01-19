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
	"github.com/kwstars/redis-dump/internal/myredis"
	"github.com/spf13/cobra"
	"log"
)

// conflictCmd represents the conflict command
var conflictCmd = &cobra.Command{
	Use:   "conflict",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		key := cmd.Flag("key").Value.String()
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

		if exist, err := sRedis.EXIST(key); err != nil {
			log.Println(err)
			return
		} else {
			if !exist {
				return
			}
		}

		if dataType, err := sRedis.TYPE(key); err != nil {
			log.Println(err)
			return
		} else {
			if dataType == "set" {
				data, err := sRedis.GetSETStrings(key)
				if err != nil {
					log.Println(err)
					return
				}
				err = dRedis.SADD(key, data)
				if err != nil {
					log.Println(err)
					return
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(conflictCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// conflictCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	//conflictCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	conflictCmd.PersistentFlags().String("source", "", "[:password@]host[:port][/database] (required)")
	conflictCmd.PersistentFlags().String("dest", "", "[:password@]host[:port][/database] (required)")
	conflictCmd.PersistentFlags().String("key", "", "[:password@]host[:port][/database] (required)")
	conflictCmd.MarkPersistentFlagRequired("s")
	conflictCmd.MarkPersistentFlagRequired("dest")
	conflictCmd.MarkPersistentFlagRequired("key")
}
