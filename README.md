# esutil
这是一个简陋的es工具类

## 使用
在`esFormatMapping.properties`文件里配置索引的mapping

格式为index_name=json

## 创建索引
```java
String index = "test";
String type = "testtype";
ElasticSearchService elasticSearchService = new ElasticSearchService();
elasticSearchService.createIndex(index, type);
```

## 加入数据
```java
Map<String,Object> mainMap = new HashMap<String,Object>();
Map<String,Object> param = new HashMap<String,Object>();
param.put("name", "张三");
param.put("num", 1);
param.put("begin_date", "2019-02-11 10:10:10");

Map<String,Object> param1 = new HashMap<String,Object>();
param1.put("name", "张三四");
param1.put("num", 2);
param1.put("begin_date", "2019-02-10 06:06:06");
mainMap.put(DigestUtils.md5Hex(JSON.toJSONString(param)), param);
mainMap.put(DigestUtils.md5Hex(JSON.toJSONString(param1)), param1);
elasticSearchService.bulkInsertData(index, type, mainMap);
```


## 基本查询
```java
ESQueryBuilderConstructor esbc = new ESQueryBuilderConstructor();
ESQueryBuilders esb = new ESQueryBuilders();
esb.term("name", "张三");
esbc.must(esb);
Map<String,Object> data = elasticSearchService.search(index, type, esbc, new String[]{"name"});
data.forEach((key,value)->{
  System.out.println(key);
  System.out.println(value);
});
```

## 分组查询
```java
ESGroupby esgb = new ESGroupby("num");
esgb.desc(2);
esgb.aggregationBuilder();
esbc.setEsGroupby(esgb);
```

---
详情请转[tonatyw的博客](https://blog.csdn.net/forintiii/article/details/91344863)
