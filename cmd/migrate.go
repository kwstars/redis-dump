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
	"github.com/spf13/cobra"
	"log"
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
	Args: func(cmd *cobra.Command, args []string) error {
		//strs := []string{"source","dest"}
		//fmt.Println(cmd.Flags().SortFlags)
		//for _, v := range strs {
		//	fmt.Println(v)
		//}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		sRedis, cf1, err1 := myredis.NewRedis("127.0.0.1", "6379", "123456")
		if err1 != nil {
			log.Println(err1)
			return
		}
		defer cf1()
		dRedis, cf2, err2 := myredis.NewRedis("127.0.0.1", "6379", "123456")
		if err2 != nil {
			log.Println(err2)
			return
		}
		defer cf2()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for v := range sRedis.Scan(ctx, 1, "*", 1000) {
			if v.Err != nil {
				log.Printf("%+v", v.Err)
				return
			}
			data, err := sRedis.DUMP(1, v.Data)
			if err != nil {
				log.Printf("%+v", err)
				return
			}
			if err := dRedis.RESTORE(11, data); err != nil {
				log.Println(err)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// migrateCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	//migrateCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	migrateCmd.PersistentFlags().String("source", "127.0.0.1:6379/0", "[:password@]host[:port][/database]")
	migrateCmd.PersistentFlags().String("dest", "127.0.0.1:6379/0", "[:password@]host[:port][/database]")
}
