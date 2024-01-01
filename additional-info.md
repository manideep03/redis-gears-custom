Java controllable RedisGears
https://github.com/dengliming/redis-modules-java/tree/master

``` python
Config config = new Config();
config.useSingleServer().setAddress("redis://127.0.0.1:6379");
RedisGearsClient redisGearsClient = new RedisGearsClient(config);

RedisGears redisGears = redisGearsClient.getRedisGears();
redisGears.pyExecute("GB().run()", false);
redisGearsClient.shutdown();
```

Redis Client
https://github.com/redisson/redisson
