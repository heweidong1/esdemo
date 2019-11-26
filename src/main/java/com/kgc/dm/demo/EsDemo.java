package com.kgc.dm.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.kgc.dm.beans.User;
import com.kgc.dm.utils.Constants;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class EsDemo
{

   private TransportClient transportClient;

   //创建连接
   public void getConnection() throws UnknownHostException {
      //put 写的是集群名字
      Settings build= Settings.builder().put("cluster.name","elasticsearch-application").build();
      transportClient =  new PreBuiltTransportClient(build)
              .addTransportAddress(
                      new TransportAddress(InetAddress.getByName("192.168.174.130"),9300));
   }
   //关闭连接
   public void closeConnection()
   {
      transportClient.close();
   }

   //通过构造器构造索引并配置

   public void addIndexSerContent() throws IOException {
      //创建索引
      transportClient.admin().indices().prepareCreate("luo_02").get();
      XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
      xContentBuilder
              //{
              .startObject()

              .startObject("properties")

              .startObject("name")
              .field("type","text")
              .field("analyzer","ik_max_word")
              .endObject()

              .startObject("sex")
              .field("type","integer")
              .endObject()

              .startObject("address")
              .field("type","text")
              .field("analyzer","ik_max_word")
              .endObject()

              .startObject("age")
              .field("type","integer")
              .endObject()

              .endObject()
               // }
              .endObject();
      //配置索引 先在luo_02索引中创建user
      transportClient.admin().indices().
              preparePutMapping("luo_02").
              setType("user").
              setSource(xContentBuilder).get();
   }

   //添加数据
   public void addDocument() throws IOException {

      XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
              .startObject()
              .field("name","weidong")
              .field("sex",1)
              .field("address","昌平区")
              .field("age",18)
              .endObject();

      transportClient.prepareIndex("luo_02","user","01")
              .setSource(xContentBuilder).get();
   }
   //添加多条数据
   public void addList()
   {
      BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
      List<User> users = JSONObject.parseArray(Constants.JSONDATA,User.class);
      for(int i=0;i<users.size();i++)
      {
         User user = users.get(i);
         String s = JSONObject.toJSONString(user);
         //可以拿到返回对象，可以查看插入结果
         //将要插入的对象 放入bulkRequestBuilder
         bulkRequestBuilder.add(
                 transportClient.prepareIndex("luo_02","user",i+"")
                         .setSource(s,XContentType.JSON)
         );
         //这是直接插入 这样拿不到结果
         //transportClient.prepareIndex("luo_02","user",i+"")
         //                         .setSource(s,XContentType.JSON).get();

      }
      //由bulkRequestBuilder 统一插入

      BulkResponse bulkItemResponses = bulkRequestBuilder.execute().actionGet();
      if(bulkItemResponses.status().getStatus()==200)
      {
         System.out.println("插入成功");
      }else
      {
         System.out.println("插入失败");
      }
   }

   public void queryAll()
   {
      //全查询
      SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch("luo_02").setTypes("user");
      searchRequestBuilder.setFrom(0);
      searchRequestBuilder.setSize(10);
      SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
      SearchHit[] hits = searchResponse.getHits().getHits();
      for(int i = 0;i<hits.length;i++)
      {
         System.out.println(hits[i].getSourceAsString());
      }
   }


   //复合查询
   public void query()
   {
      //创建连接 GET luo_02/user/_search
      SearchRequestBuilder luo_02 = transportClient.prepareSearch("luo_02").setTypes("user");
      /**
       * {
       *     "from":0,
       *     "size": 20
       * }
       */
      luo_02.setFrom(0);
      luo_02.setSize(50);

      //创建bool 复合查询
      BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

      //创建模糊查询
      MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "李");
      //规定bool 必须 满足 模糊查询
      boolQueryBuilder.must(matchQueryBuilder);
      //规定bool 过滤查询  过滤查询的添加时 term sex= 1；
      boolQueryBuilder.filter(QueryBuilders.termQuery("sex",1));

      luo_02.setQuery(boolQueryBuilder);

      /**
       *
       GET luo_02/user/_search
       {
       "from":0,
       "size": 20
       "query": {
             "bool":
             {
                "must":
                   [
                      {
                         "match": {
                         "name": "李"
                         }
                      }
                   ],
                   "filter": {
                      "term": {
                      "sex": 1
                      }
                  }
             }
          }
       }

       */

      SearchResponse searchResponse = luo_02.execute().actionGet();
      //获取查询结果
      SearchHit[] hits = searchResponse.getHits().getHits();
      for(int i = 0;i<hits.length;i++)
      {
         System.out.println(hits[i].getSourceAsString());
      }
   }


   public static void main(String[] args) throws IOException {
      EsDemo esDemo = new EsDemo();
      //创建连接
      esDemo.getConnection();
      //创建索引 配置索引
      //esDemo.addIndexSerContent();

      //添加数据
      //esDemo.addList();

      //查询所有
      esDemo.query();

      //关闭连接
      esDemo.closeConnection();
   }
}
