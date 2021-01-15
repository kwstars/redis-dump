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

func NewRedis(ip, port, password string) (r *Redis, cf func(), err error) {
	var addr = net.JoinHostPort(ip, port)

	r = &Redis{pool: &redis.Pool{
		MaxIdle:     3, // 不加会有这个错误 read tcp 127.0.0.1:54116->127.0.0.1:6379: read: connection reset by peer
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr, redis.DialPassword(password))
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

func (r *Redis) GetConfig(key string) (dbs string, err error) {
	var conn = r.pool.Get()
	defer conn.Close()

	n, err := redis.StringMap(conn.Do("CONFIG", "GET", key))
	if err != nil {
		return "", errors.Wrap(err, "Receive")
	}

	return n[key], nil
}

func (r *Redis) GetSETStrings(db int, key string) (data []string, err error) {
	var conn = r.pool.Get()
	defer conn.Close()

	if err = conn.Send("SELECT", db); err != nil {
		return nil, errors.Wrapf(err, "SELECT, db: %d, key: %s", db, key)
	}
	if err = conn.Send("SMEMBERS", key); err != nil {
		return nil, errors.Wrapf(err, "SMEMBERS, db: %d, key: %s", db, key)
	}

	if err = conn.Flush(); err != nil {
		return nil, errors.Wrapf(err, "Flush, db: %d, key: %s", db, key)
	}

	if _, err := conn.Receive(); err != nil {
		return nil, errors.Wrapf(err, "SELECT, db: %d, key: %s", db, key)
	}
	if data, err = redis.Strings(conn.Receive()); err != nil {
		return nil, errors.Wrapf(err, "SMEMBERS, db: %d, key: %s", db, key)
	}

	return
}

type SCANResult struct {
	Data []string
	Err  error
}

func (r *Redis) Scan(ctx context.Context, db int, match string, count int) <-chan SCANResult {
	var results = make(chan SCANResult)
	go func() {
		defer close(results)

		var conn = r.pool.Get()
		defer conn.Close()
		if err := conn.Send("SELECT", db); err != nil {
			results <- SCANResult{Data: nil, Err: errors.WithStack(err)}
		}

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

func (r *Redis) DUMP(db int, keys []string) (data map[string][]byte, err error) {
	var conn = r.pool.Get()
	defer conn.Close()

	data = make(map[string][]byte)
	if err = conn.Send("SELECT", db); err != nil {
		return nil, errors.WithStack(err)
	}

	for _, key := range keys {
		err := conn.Send("DUMP", key)
		if err != nil {
			return nil, errors.Errorf("%s: %v", key, err)
		}
	}

	if err = conn.Flush(); err != nil {
		return nil, errors.WithStack(err)
	}

	if _, err := conn.Receive(); err != nil {
		return nil, errors.WithStack(err)
	}

	for i := 0; i < len(keys); i++ {
		if b, err := redis.Bytes(conn.Receive()); err != nil {
			return nil, errors.Errorf("%s: %v", keys[i], err)
		} else {
			data[keys[i]] = b
		}
	}
	return data, nil
}

func (r *Redis) RESTORE(db int, data map[string][]byte) (err error) {
	var conn = r.pool.Get()
	defer conn.Close()
	tmp := make([]string, 0, len(data))
	if err = conn.Send("SELECT", db); err != nil {
		return errors.WithStack(err)
	}

	for k, v := range data {
		// 0是ttl
		tmp = append(tmp, k)
		if err := conn.Send("RESTORE", k, 0, v); err != nil {
			return errors.Errorf("%s: %v", k, err)
		}
	}

	if err = conn.Flush(); err != nil {
		return errors.WithStack(err)
	}

	for i := 0; i < len(data); i++ {
		if _, err := conn.Receive(); err != nil {
			return errors.Errorf("%s: %v", tmp[i], err)
		}
	}

	return nil
}
