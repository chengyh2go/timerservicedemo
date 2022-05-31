package com.xysec.timerservice.config;

import java.util.Properties;

public interface Config {

    //解析配置
     Properties getConfig(String[] args);

}
