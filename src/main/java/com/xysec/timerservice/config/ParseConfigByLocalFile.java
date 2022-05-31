package com.xysec.timerservice.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ParseConfigByLocalFile implements Config {

    @Override
    public Properties getConfig(String[] args) {
        //加载配置文件，获取全局配置参数
        InputStream is = ParseConfigByLocalFile.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
