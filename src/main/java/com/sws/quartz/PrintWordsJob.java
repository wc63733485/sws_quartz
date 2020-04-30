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
import java.util.*;

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

        for (String s : codelist) {
            MongoDatabase device = mongoClient.getDatabase(dbName);

            MongoCollection<Document> collection = device.getCollection(s);

            Document document = new Document();
            document.put("$or", Arrays.asList(new Document("no1_frequency_oper", 1), new Document("no1_powerfrequency_oper", 1)));
            long l = collection.countDocuments(document);
            Document document1 = new Document();
            document1.put("$or", Arrays.asList(new Document("no2_frequency_oper", 1), new Document("no2_powerfrequency_oper", 1)));
            long l1 = collection.countDocuments(document1);
            Document document2 = new Document();
            document2.put("$or", Arrays.asList(new Document("no3_frequency_oper", 1), new Document("no3_powerfrequency_oper", 1)));
            long l2 = collection.countDocuments(document2);

            String strDateFormat = "yyyyMMdd";
            SimpleDateFormat sdf = new SimpleDateFormat(strDateFormat);
            String format = sdf.format(new Date(TimeUtil.getLastDayStartMills()));
            device.createCollection(format);
            MongoCollection<Document> collection1 = device.getCollection(format);
            Document insert = new Document();
            insert.put("pumpOneRunTime",new Document().append("pumpOne",l).append("pumpTwo",l1).append("pumpThree",l2));
            collection1.insertOne(insert);
        }


        String printTime = new SimpleDateFormat("yy-MM-dd HH-mm-ss").format(new Date());
        System.out.println("PrintWordsJob start at:" + printTime + ", prints: Hello Job-" + new Random().nextInt(100));
    }
}
