# ElasticSearch
> ElasticSearch 处理服务 满足output逻辑

## 内置方法
- [vela.elastic.cli(cfg)](#客户端) &emsp;elastic 客户端
- [vela.elastic.index(string...)](#索引函数) &emsp; 索引函数
- [vela.elastic.drop] &emsp; 删除动作
- [kafka样例] &emsp;kafka消费


## 客户端
> cli = vela.elastic.cli(cfg) <br />
> cfg: 配置信息 返回:[客户端](#)

配置参数:

- index  &emsp;入库索引:"%s-app-%s"
- username
- password
- tls_ca
- tls_cert
- tls_key
- tls_min_version
- ssl_ca
- ssl_cert
- ssl_key
- server_name
- insecure_skip_verify
- timeout
- enable_sniffer
- enable_gzip
- auth_bearer_token
- url
- thread
- interval
- flush
>

配置函数:
- [send([doc](#doc))](#)
- [index(string...)](#)
- [drop(cnd)](#)
- [switch(switch)](#)
- [clone(string)](#) &emsp;clone一个新的client
>

```lua
    local index = vela.elastic.index

    local cli = vela.elastic.cli{
        url = "http://localhost:9092",
    }

    cli.index("%s-app-%s" , "$day" , "app")
    cli.drop("host = www.baidu.com")

    local vsh = vela.switch() -- 新增switch
    vsh.case("app = www.baidu.com").pipe(index("app-%s" , "$day"))
    cli.switch(vsh)
```

## 索引函数
> index = vela.elastic.index(format , string...) <br />
> format:索引模板 string:关键字 用$符号作为变量前缀 [doc](#doc)的字段
```lua
    local index = vela.elastic.index("%s-guba" , "$day")
    
    local cli = vela.elastic.cli{}
    cli.index = index
    
```

## kafka样例
> 将kafka内容消费到Elastic

```lua

    
    local index = vela.elastic.index
    local cli = vela.elastic.cli{
        url = "http://127.0.0.1:9200"
    }
    
    cli.index = index("%s-app" , "$day")
    
    local kfk = vela.kfk.consumer{
        name = "kfk",
        addr =  {"127.0.0.1:9092" , "192.168.1.1:9092"},
        topic = "app",
    }
    kfk.to(cli)
    kfk.start()
```