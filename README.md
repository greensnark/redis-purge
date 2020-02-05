# redis-purge

A simple program to look through redis for keys with bad string values and
optionally delete those keys

## Usage

    [REDIS_ADDR=...]           \
    [DELETE_MATCHING_KEYS=yes] \
    [REQUIRED_MATCH_COUNT=n]   \
    [SIZE_THRESHOLD=x]         \
    	redis-purge [value]

Deletes all keys with a given value if run with `DELETE_MATCHING_KEYS=yes`
or `DELETE_MATCHING_KEYS=y` in the environment, otherwise lists the keys with
the given value.

If `SIZE_THRESHOLD` is set to a number of bytes in the environment, only keys
with values at least as large as `SIZE_THRESHOLD` will be considered.

If `REQUIRED_MATCH_COUNT` is a number >0, then keys are selected if the value
contains the search pattern _at least_ that many times.

### Examples

Delete all keys with value set to the string "null", connecting to the
redis server on port 6379 on any interface:

    DELETE_MATCHING_KEYS=yes redis-purge null


List all keys with value set to the string "null", connecting to the redis
server on `redis:6379`. Matching keys *won't* be deleted, only listed:

    REDIS_ADDR=redis:6379 redis-purge null

Delete all keys larger than 20,000 bytes that contain repetitions of a bad
search string ("badvalue") at least three times:

    DELETE_MATCHING_KEYS=yes REQUIRED_MATCH_COUNT=3 SIZE_THRESHOLD=20000 \
        redis-purge badvalue
