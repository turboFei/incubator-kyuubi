# Kyuubi-data-api

Refer [carmel-data-api](https://github.corp.ebay.com/carmel/carmel-data-api) and make some change for kyuubi.

## Docs

https://pages.github.corp.ebay.com/hadoop/kyuubi-docs/ebay_user_guide/java_jdbc_ebay.html
https://wiki.vip.corp.ebay.com/display/DW/Upload+API
https://wiki.vip.corp.ebay.com/display/DW/Download+API

## Upload

```shell
# see usage with `./bin/upload -h`
./bin/upload -u "jdbc:hive2://kyuubi.hadoop.qa.ebay.com:10009/default#kyuubi.session.cluster=zeuslvs;spark.yarn.queue=hdmi-data-default" \
    -t p_kyuubi.iris_data -P "dt=20200303,n=1000" -O "header=false,delimiter=," -o \
    -f /Users/xingczhang/test/iris.data.csv /Users/xingczhang/test/iris.data.csv2
```

## Download

```shell
# see usage with `./bin/download -h`
./bin/download -u "jdbc:hive2://kyuubi.hadoop.qa.ebay.com:10009/default#kyuubi.session.cluster=zeuslvs;spark.yarn.queue=hdmi-data-default" \
    -q "select  id, uuid() as uuid, now() as n  from  range(10)" \
    -d /tmp/spark/dest
gzip -d /tmp/spark/dest/*.gz
```
