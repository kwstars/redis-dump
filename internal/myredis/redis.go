package myredis

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"log"
	"net"
	"time"
)

type Redis struct {
	pool *redis.Pool
}

func NewRedis(ip, port, password string, db int) (r *Redis, cf func(), err error) {
	var addr = net.JoinHostPort(ip, port)

	r = &Redis{pool: &redis.Pool{
		MaxIdle:     3, // 不加会有这个错误 read tcp 127.0.0.1:54116->127.0.0.1:6379: read: connection reset by peer
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr, redis.DialPassword(password), redis.DialDatabase(db))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}}

	cf = func() {
		if err := r.pool.Close(); err != nil {
			log.Println(err)
		}
	}

	return
}

//https://raw.githubusercontent.com/redis/redis/2.8/redis.conf
func (r *Redis) GetConfig(confKey string) (conf string, err error) {
	var conn = r.pool.Get()
	defer conn.Close()

	allConfig, err := redis.StringMap(conn.Do("CONFIG", "GET", confKey))
	if err != nil {
		return "", errors.WithStack(err)
	}
	resp, ok := allConfig[confKey]
	if !ok {
		return "", errors.Errorf("No found confKey: %v", confKey)
	}

	return resp, nil
}

func (r *Redis) GetSETStrings(key string) (data []string, err error) {
	var conn = r.pool.Get()
	defer conn.Close()

	if data, err = redis.Strings(conn.Do("SMEMBERS", key)); err != nil {
		return nil, errors.WithStack(err)
	}

	return
}

type SCANResult struct {
	Data []string
	Err  error
}

func (r *Redis) SCAN(ctx context.Context, match string, count int) <-chan SCANResult {
	var results = make(chan SCANResult)
	go func() {
		defer close(results)

		var conn = r.pool.Get()
		defer conn.Close()

		var cursor = 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				values, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", match, "COUNT", count))
				if err != nil {
					results <- SCANResult{Data: nil, Err: errors.WithStack(err)}
					return
				}

				for len(values) > 0 {
					v := []string{}
					values, err = redis.Scan(values, &cursor, &v)
					if err != nil {
						results <- SCANResult{Data: nil, Err: errors.WithStack(err)}
						return
					}
					results <- SCANResult{Data: v, Err: nil}
				}

				if cursor == 0 {
					return
				}
			}
		}
	}()

	return results
}

func (r *Redis) DUMP(keys []string) (data map[string][]byte, err error) {
	var conn = r.pool.Get()
	defer conn.Close()

	data = make(map[string][]byte)

	for _, key := range keys {
		if b, err := redis.Bytes(conn.Do("DUMP", key)); err != nil {
			return nil, errors.Errorf("%s: %v", key, err)
		} else {
			data[key] = b
		}
	}

	return data, nil
}

func (r *Redis) RESTORE(data map[string][]byte) (err error) {
	var conn = r.pool.Get()
	defer conn.Close()

	for k, v := range data {
		// 0是ttl
		if _, err := conn.Do("RESTORE", k, 0, v); err != nil {
			log.Printf("%s: %v", k, err)
		}
	}
	return
}

func (r *Redis) EXIST(key string) (exist bool, err error) {
	var conn = r.pool.Get()
	defer conn.Close()

	if exist, err = redis.Bool(conn.Do("EXISTS", key)); err != nil {
		return false, err
	}

	return exist, nil
}

func (r *Redis) TYPE(key string) (dataType string, err error) {
	var conn = r.pool.Get()
	defer conn.Close()

	if dataType, err = redis.String(conn.Do("TYPE", key)); err != nil {
		return "", err
	} else {
		return dataType, nil
	}
}

func (r *Redis) SADD(key string, data []string) (err error) {
	var conn = r.pool.Get()
	defer conn.Close()
	lenght := len(data)

	for _, v := range data {
		if err := conn.Send("SADD", key, v); err != nil {
			return errors.WithStack(err)
		}
	}

	if err := conn.Flush(); err != nil {
		return errors.WithStack(err)
	}

	for i := 0; i < lenght; i++ {
		if _, err := conn.Receive(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
