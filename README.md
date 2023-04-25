NimoShake: sync from dynamodb to mongodb, including full sync and increase sync.<br>
NimoFullCheck: check data consistency after syncing.<br>
These two tools are developed and maintained by NoSQL Team in Alibaba-Cloud Database department.<br>

* [中文文档介绍](https://yq.aliyun.com/articles/717439)
* [binary download](https://github.com/alibaba/NimoShake/releases)

#Usage
---
NimoShake:
`./nimo-shake -conf=nimo-shake.conf`<br>
NimoFullCheck:
`./nimo-full-check --sourceAccessKeyID="xxx" --sourceSecretAccessKey="xxx" --sourceRegion="us-east-2" --filterTablesWhiteList="table list" -t="target mongodb address"`

# Shake series tool
---
We also provide some tools for synchronization in Shake series.<br>

* [MongoShake](https://github.com/aliyun/MongoShake): mongodb data synchronization tool.
* [RedisShake](https://github.com/aliyun/RedisShake): redis data synchronization tool.
* [RedisFullCheck](https://github.com/aliyun/RedisFullCheck): redis data synchronization verification tool.
* [NimoShake](https://github.com/alibaba/NimoShake): sync dynamodb to mongodb.


