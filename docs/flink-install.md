# flink 安装

我使用的是 Mac，这里只能总结一下 Mac 环境下 flink 的安装流程

## 环境依赖

flink 的安装依赖于 Java 环境，首先查看是否安装过 Java

```
$ java -version

java version "1.8.0_191"
Java(TM) SE Runtime Environment (build 1.8.0_191-b12)
Java HotSpot(TM) 64-Bit Server VM (build 25.191-b12, mixed mode)
```

## 安装 flink

mac 下安装 flink 非常方便，brew 一键就可以安装

```
$ brew install apache-flink
...

$ flink --version
Version: 1.7.2, Commit ID: ceba8af
```

## 启动 flink

可以通过 `brew info apache-flink` 查询 flink 的安装目录

```
$ brew info apache-flink
apache-flink: stable 1.7.2, HEAD
Scalable batch and stream data processing
https://flink.apache.org/
/usr/local/Cellar/apache-flink/1.7.2 (174 files, 321.8MB)
```

然后进入 flink 的目录，进入 /libexec/bin 子目录，执行 `start-cluster.sh` 就能启动 flink，执行 `stop-cluster.sh` 关闭 flink

## 进入 flink 提供的 web 交互页面

启动 flink 之后，接着就可以进入web页面 (http://localhost:8081/)