package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/go-redis/redis"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	redisDB := redis.NewClient(redisOptions())
	defer redisDB.Close()

	searchValue := os.Args[1]

	search := redisSearch{
		Client: redisDB,
		Debug:  os.Getenv("DEBUG") != "",
	}

	if os.Getenv("DELETE_MATCHING_KEYS") == "yes" {
		reportError("error deleting keys matching: "+searchValue, search.deleteMatchingKeys(searchValue))
	} else {
		reportError("error listing keys mathcing: "+searchValue, search.listMatchingKeys(searchValue))
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: [REDIS_ADDR=...] [DELETE_MATCHING_KEYS=yes] %s [value]

Deletes all keys with a given value if run with DELETE_MATCHING_KEYS=yes in the
environment, otherwise lists the keys with the given value.
`,
		os.Args[0])

	os.Exit(1)
}

type redisSearch struct {
	Client *redis.Client
	Debug  bool
}

func (r redisSearch) matchingKeysDo(searchValue string, action func(key string, value []byte) error) error {
	searchBytes := []byte(searchValue)

	var scanCursor uint64
	var keys []string
	var err error

	for {
		keys, scanCursor, err = r.Client.Scan(scanCursor, "", 50).Result()
		if err != nil {
			return err
		}
		if r.Debug {
			fmt.Fprintf(os.Stderr, "> scan cursor: %d, key count: %d\n", scanCursor, len(keys))
		}

		for _, key := range keys {
			value, err := r.fetchValue(key)
			if err != nil {
				fmt.Fprintf(os.Stderr, "> error reading %#v, skipping\n", key)
				continue
			}

			if bytes.Equal(value, searchBytes) {
				if err = action(key, value); err != nil {
					return err
				}
			}
		}

		if scanCursor == 0 {
			break
		}
	}
	return nil
}

func (r redisSearch) deleteMatchingKeys(searchValue string) error {
	var deletedKeyCount, failedDeleteCount int64

	fmt.Fprintf(os.Stderr, "> deleting keys with value == %#v\n", searchValue)
	defer func() {
		fmt.Fprintf(os.Stderr, "> deleted %d keys matching %#v, %d keys failed delete\n", deletedKeyCount, searchValue, failedDeleteCount)
	}()

	return r.matchingKeysDo(searchValue, func(key string, value []byte) error {
		fmt.Println("DELETE", key)
		if err := r.deleteKey(key); err != nil {
			fmt.Fprintf(os.Stderr, "> failed to delete key %#v: %s, continuing\n", key, err)
			failedDeleteCount++
		} else {
			deletedKeyCount++
		}
		return nil
	})
}

func (r redisSearch) listMatchingKeys(searchValue string) error {
	var matchingKeyCount int64

	fmt.Fprintf(os.Stderr, "> listing keys with value == %#v\n", searchValue)
	defer func() {
		fmt.Fprintf(os.Stderr, "> found %d keys matching %#v\n", matchingKeyCount, searchValue)
	}()

	return r.matchingKeysDo(searchValue, func(key string, value []byte) error {
		fmt.Println(key)
		matchingKeyCount++
		return nil
	})
}

func (r redisSearch) fetchValue(key string) ([]byte, error) {
	return r.Client.Get(key).Bytes()
}

func (r redisSearch) deleteKey(key string) error {
	return r.Client.Del(key).Err()
}
func redisOptions() *redis.Options {
	return &redis.Options{
		Addr: envDefault("REDIS_ADDR", ":6379"),
	}
}

func envDefault(envname string, defaultValue string) string {
	envvalue := os.Getenv(envname)
	if envvalue == "" {
		return defaultValue
	}
	return envvalue
}

func reportError(message string, err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "> %s: %s\n", message, err)
	os.Exit(1)
}
