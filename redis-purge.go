package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	redisDB := redis.NewClient(redisOptions())
	defer redisDB.Close()

	search := redisSearch{
		Client: redisDB,
		Debug:  os.Getenv("DEBUG") != "",
	}

	needle := &searchCondition{
		Search:        os.Args[1],
		SizeThreshold: envInt("SIZE_THRESHOLD", 0),
		Occurrences:   envInt("REQUIRED_MATCH_COUNT", 0),
	}

	if envBool("DELETE_MATCHING_KEYS") {
		reportError("error deleting keys matching: "+needle.String(), search.deleteMatchingKeys(needle))
	} else {
		reportError("error listing keys matching: "+needle.String(), search.listMatchingKeys(needle))
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:

[REDIS_ADDR=...]           \
[DELETE_MATCHING_KEYS=yes] \
[REQUIRED_MATCH_COUNT=n]   \
[SIZE_THRESHOLD=x]         \
	%s [value]

Deletes all keys with a given value if run with DELETE_MATCHING_KEYS=yes
or DELETE_MATCHING_KEYS=y in the environment, otherwise lists the keys with
the given value.

If SIZE_THRESHOLD is set to a number of bytes in the environment, only keys
with values at least as large as SIZE_THRESHOLD will be considered.

If REQUIRED_MATCH_COUNT is a number >0, then keys are selected if the value
contains the search pattern _at least_ that many times.
`,
		os.Args[0])

	os.Exit(1)
}

func envInt(name string, defval int) (intValue int) {
	var err error
	intValue, err = strconv.Atoi(os.Getenv(name))
	if err != nil {
		return defval
	}
	return intValue
}

func envBool(name string) bool {
	value := strings.ToLower(os.Getenv(name))
	return value == "y" || value == "yes" || value == "true" || value == "t" || value == "1"
}

type redisSearch struct {
	Client *redis.Client
	Debug  bool
}

// A searchCondition specifies how to match a Redis value of interest
type searchCondition struct {
	// SizeThreshold is the minimum size of a search value to be considered
	SizeThreshold int

	// Search is the exact or substring match for a value to be considered
	Search string

	// Occurrences is the minimum number of occurrences of a search string
	// for a value to be considered. If Occurrences == 0, requires an exact
	// match of Search to the value.
	Occurrences int
}

func (s *searchCondition) searchDescription() string {
	if s.Search == "" {
		return "(any)"
	}
	return fmt.Sprintf("%#v", s.Search)
}

func (s *searchCondition) String() string {
	var description bytes.Buffer
	fmt.Fprintf(&description, "Search=%s", s.searchDescription())
	if s.SizeThreshold > 0 {
		fmt.Fprintf(&description, " (size >= %d bytes)", s.SizeThreshold)
	}
	if s.Search != "" {
		if s.Occurrences <= 0 {
			fmt.Fprint(&description, " (exact match)")
		} else {
			fmt.Fprintf(&description, " (match >= %d occurrences)", s.Occurrences)
		}
	}
	return description.String()
}

// Matcher returns a function that accepts a Redis key's value and returns
// true if the value satisfies the searchCondition s
func (s *searchCondition) Matcher() func(value []byte) bool {
	searchBytes := []byte(s.Search)

	return func(value []byte) bool {
		if len(value) < s.SizeThreshold {
			return false
		}

		if len(searchBytes) == 0 {
			return true
		}

		if s.Occurrences <= 0 {
			return bytes.Equal(value, searchBytes)
		}

		return bytes.Count(value, searchBytes) >= s.Occurrences
	}
}

func (r redisSearch) matchingKeysDo(search *searchCondition, action func(key string, value []byte) error) error {
	valueMatches := search.Matcher()

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
				fmt.Fprintf(os.Stderr, "> fetchValue error reading %#v (%s), skipping\n", key, err)
				continue
			}

			if valueMatches(value) {
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

func average(sum, n int64) float64 {
	if n == 0 {
		return 0.0
	}
	return float64(sum) / float64(n)
}

func (r redisSearch) deleteMatchingKeys(search *searchCondition) error {
	var deletedKeyCount, deletedValuesTotalSize, failedDeleteCount int64

	fmt.Fprintf(os.Stderr, "> deleting keys with value matching %s\n", search)
	defer func() {
		fmt.Fprintf(os.Stderr, "> deleted %d keys (%d total size, average size: %.1f) matching %s, %d keys failed delete\n",
			deletedKeyCount, deletedValuesTotalSize, average(deletedValuesTotalSize, deletedKeyCount), search, failedDeleteCount)
	}()

	return r.matchingKeysDo(search, func(key string, value []byte) error {
		fmt.Println("DELETE", key)
		if err := r.deleteKey(key); err != nil {
			fmt.Fprintf(os.Stderr, "> failed to delete key %#v: %s, continuing\n", key, err)
			failedDeleteCount++
		} else {
			deletedKeyCount++
			deletedValuesTotalSize += int64(len(value))
		}
		return nil
	})
}

func (r redisSearch) listMatchingKeys(search *searchCondition) error {
	var matchingKeyCount, matchingValuesTotalSize int64

	fmt.Fprintf(os.Stderr, "> listing keys with value matching %s\n", search)
	defer func() {
		fmt.Fprintf(os.Stderr, "> found %d keys (total size: %d, average size: %.1f) matching %s\n",
			matchingKeyCount, matchingValuesTotalSize, average(matchingValuesTotalSize, matchingKeyCount), search)
	}()

	return r.matchingKeysDo(search, func(key string, value []byte) error {
		fmt.Println(key)
		matchingKeyCount++
		matchingValuesTotalSize += int64(len(value))
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
