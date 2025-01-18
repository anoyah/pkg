package for golang of common business

**Package Structure**
```shell
├── LICENSE # license file
├── README.md # ...
├── go.mod # ...
├── go.sum # ...
├── pkgs.go # entry file
└── redis # redis tool
    ├── log.go # custom log 
    ├── mutex # mutex module
    │   ├── mutex.go # default mutex
    │   ├── mutex_etcd.go # etcd implements for mutex
    │   ├── mutex_redis.go # redis implements for mutex
    │   └── mutex_test.go # mutex test file
    ├── redis.go # main file of redis tools
    └── redis_test.go # test file for redis.go
```

## 1. Redis
> redis tools implements for common business, maybe more like as snippet for the project.

### 1. Cache
