master server 第一个 etcd 节点启动命令：
```shell
bash master.sh [节点IP]
```
region server 后续接入 etcd 节点命令：
```shell
bash region.sh [节点IP] [master IP]
```
