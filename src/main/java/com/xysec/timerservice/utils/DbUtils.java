package com.xysec.timerservice.utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DbUtils {
    private static final Logger logger = LoggerFactory.getLogger(DbUtils.class);

    public static QueryRunner getQueryRunner(String configFile) throws Exception {
        Properties druidProp = new Properties();
        InputStream is = DbUtils.class.getClassLoader().getResourceAsStream(configFile);
        druidProp.load(is);
        DataSource dataSource = DruidDataSourceFactory.createDataSource(druidProp);
        return new QueryRunner(dataSource);
    }

    public static void execUpdate(QueryRunner queryRunner,String sql)  {
        try {
            queryRunner.update(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static String execReturnSingleStringResultQuery(QueryRunner queryRunner,String sql)  {
        String res = "";
        try {
            res = queryRunner.query(sql, new ScalarHandler<>());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res;
    }
}
