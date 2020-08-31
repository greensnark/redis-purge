package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	redisDB := redis.NewClient(redisOptions())
	defer redisDB.Close()

	search := redisSearch{
		Client:   redisDB,
		Debug:    os.Getenv("DEBUG") != "",
		Progress: envBool("PROGRESS", "true"),
	}

	needle := &searchCondition{
		AccessMode:    parseValueAccessMode(os.Getenv("ACCESS_MODE")),
		Search:        os.Args[1],
		SizeThreshold: envInt("SIZE_THRESHOLD", 0),
		Occurrences:   envInt("REQUIRED_MATCH_COUNT", 0),
	}

	if envBool("DELETE_MATCHING_KEYS", "false") {
		reportError("error deleting keys matching: "+needle.String(), search.deleteMatchingKeys(needle))
	} else {
		reportError("error listing keys matching: "+needle.String(), search.listMatchingKeys(needle))
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:

[REDIS_ADDR=...]           \
[ACCESS_MODE=string|hash]  \
[DELETE_MATCHING_KEYS=yes] \
[REQUIRED_MATCH_COUNT=n]   \
[SIZE_THRESHOLD=x]         \
	%s [value]

Deletes all keys with a given value if run with DELETE_MATCHING_KEYS=yes
or DELETE_MATCHING_KEYS=y in the environment, otherwise lists the keys with
the given value.

If ACCESS_MODE is hash, values will be treated as redis hashes. If ACCESS_MODE
is string, values will be treated as simple strings. If unspecified, 
ACCESS_MODE defaults to hash.

If SIZE_THRESHOLD is set to a number of bytes in the environment, only keys
with values at least as large as SIZE_THRESHOLD will be considered.

If REQUIRED_MATCH_COUNT is a number >0, then keys are selected if the value
contains the search pattern _at least_ that many times.

[value] is required to be an exact string match to the redis key's value if
REQUIRED_MATCH_COUNT is not set. If REQUIRED_MATCH_COUNT is set, [value] is
required to be a simple substring of the redis key's value with at least
REQUIRED_MATCH_COUNT occurrences.
`,
		os.Args[0])

	os.Exit(1)
}

type valueAccessMode int

const (
	valueAccessString valueAccessMode = iota
	valueAccessHash
)

func (v valueAccessMode) String() string {
	switch v {
	case valueAccessString:
		return "string"
	case valueAccessHash:
		return "hash"
	default:
		return "?"
	}
}

func (v valueAccessMode) Get(c *redis.Client, key string) (body []byte, err error) {
	switch v {
	case valueAccessString:
		return c.Get(key).Bytes()
	case valueAccessHash:
		hashValue, err := c.HGetAll(key).Result()
		if err != nil {
			return nil, fmt.Errorf("valueAccessHash[%#v]: %w", key, err)
		}
		return hashAsBytes(hashValue), nil
	}
	panic(fmt.Sprintf("impossible valueAccessMode: %d", v))
}

func hashAsBytes(valueHash map[string]string) []byte {
	byteBuf := &bytes.Buffer{}
	for key, value := range valueHash {
		byteBuf.Write([]byte(key))
		byteBuf.Write([]byte(value))
	}
	return byteBuf.Bytes()
}

func parseValueAccessMode(accessMode string) valueAccessMode {
	switch strings.ToLower(accessMode) {
	case "string":
		return valueAccessString
	default:
		return valueAccessHash
	}
}

type redisSearch struct {
	Client   *redis.Client
	Debug    bool
	Progress bool
}

// A searchCondition specifies how to find a Redis value of interest
type searchCondition struct {
	// AccessMode specifies how redis values should be read, whether
	// as simple strings, or hashes.
	AccessMode valueAccessMode

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
	fmt.Fprintf(&description, "(access-mode=%s) Search=%s", s.AccessMode.String(), s.searchDescription())
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

func (r redisSearch) countKeys() (int64, error) {
	return r.Client.DBSize().Result()
}

func (r redisSearch) matchingKeysDo(search *searchCondition, action func(key string, value []byte) error) error {
	valueMatches := search.Matcher()

	var scanCursor uint64
	var keys []string
	var err error

	totalKeys, err := r.countKeys()
	if err != nil {
		return fmt.Errorf("couldn't count keys: %w", err)
	}

	var visitingKeys int64

	for {
		keys, scanCursor, err = r.Client.Scan(scanCursor, "", 50).Result()
		if err != nil {
			return err
		}
		if r.Debug {
			fmt.Fprintf(os.Stderr, "> scan cursor: %d, key count: %d\n", scanCursor, len(keys))
		}

		if r.Progress {
			fmt.Fprintf(os.Stderr, "Visiting keys %d-%d of %d (%.2f%%)\r",
				visitingKeys, visitingKeys+int64(len(keys)), totalKeys,
				percentage(visitingKeys+int64(len(keys)), totalKeys))
		}
		visitingKeys += int64(len(keys))

		for _, key := range keys {
			value, err := r.fetchValue(key, search.AccessMode)
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

func percentage(num, den int64) float64 {
	if den == 0 {
		return 0.0
	}
	return float64(num) * 100.0 / float64(den)
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
		fmt.Printf("DELETE %s (size = %d)\n", key, len(value))
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
		fmt.Printf("%s (size = %d)\n", key, len(value))
		matchingKeyCount++
		matchingValuesTotalSize += int64(len(value))
		return nil
	})
}

func (r redisSearch) fetchValue(key string, accessMode valueAccessMode) ([]byte, error) {
	return accessMode.Get(r.Client, key)
}

func (r redisSearch) deleteKey(key string) error {
	return r.Client.Del(key).Err()
}

func redisOptions() *redis.Options {
	return &redis.Options{
		Addr:        envDefault("REDIS_ADDR", ":6379"),
		ReadTimeout: time.Duration(envInt("READ_TIMEOUT", 180)) * time.Second,
	}
}

func envDefault(envname string, defaultValue string) string {
	envvalue := os.Getenv(envname)
	if envvalue == "" {
		return defaultValue
	}
	return envvalue
}

func envInt(name string, defval int) (intValue int) {
	var err error
	intValue, err = strconv.Atoi(os.Getenv(name))
	if err != nil {
		return defval
	}
	return intValue
}

func envBool(name, defval string) bool {
	value := strings.ToLower(envDefault(name, defval))
	return value == "y" || value == "yes" || value == "true" || value == "t" || value == "1"
}

func reportError(message string, err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "> %s: %s\n", message, err)
	os.Exit(1)
}
