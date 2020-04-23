package com.sws.quartz;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sws.base.util.SqlUtil;
import com.sws.base.util.TimeUtil;
import org.bson.Document;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class PrintWordsJob implements Job {

    private String dbName = "device";

    private String[] uri = {"39.96.74.32:27837"};

    private String username = "root";

    private String password = "ASDzxc1993";


    public List<ServerAddress> getServerAddressList() {

        List<ServerAddress> serverAddressList = new ArrayList<>();
        ServerAddress serverAddress = null;
        for (Object object : uri) {
            serverAddress = new ServerAddress(object.toString());
            serverAddressList.add(serverAddress);
        }

        return serverAddressList;
    }

    private static final SqlUtil sqlUtil = new SqlUtil();
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUrl("jdbc:mysql://39.96.74.32:25412/hssws?allowPublicKeyRetrieval=true&serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&useSSL=false");
        ds.setUsername("root");
        ds.setPassword("ASDzxc1993.");

        JdbcTemplate jdbcTemplate = new JdbcTemplate();
        jdbcTemplate.setDataSource(ds);

        List<ServerAddress> listHost = getServerAddressList();
        MongoClientOptions.Builder options = new MongoClientOptions.Builder();
        options.connectionsPerHost(300);// 连接池设置为300个连接,默认为100
        options.connectTimeout(15000);// 连接超时，推荐>3000毫秒
        options.maxWaitTime(5000); //
        options.socketTimeout(0);// 套接字超时时间，0无限制
        options.threadsAllowedToBlockForConnectionMultiplier(5000);// 线程队列数，如果连接线程排满了队列就会抛出“Out of semaphores to get db”错误。
        options.writeConcern(WriteConcern.SAFE);//
        MongoClientOptions build = options.build();
        MongoCredential mongoCredential = MongoCredential.createCredential(username, "admin", password.toCharArray());
        MongoClient mongoClient = new MongoClient(listHost, mongoCredential, build);


        List<String> codelist = jdbcTemplate.queryForList("select distinct code from device", String.class);

        for (String s:codelist) {
            MongoDatabase device = mongoClient.getDatabase(dbName);
            MongoCollection<Document> collection = device.getCollection(s);
            BasicDBObject bso = new BasicDBObject();
            bso.append("", TimeUtil.getTodayStartMills());
            collection.countDocuments();
        }


        String printTime = new SimpleDateFormat("yy-MM-dd HH-mm-ss").format(new Date());
        System.out.println("PrintWordsJob start at:" + printTime + ", prints: Hello Job-" + new Random().nextInt(100));
    }
}
