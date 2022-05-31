package com.xysec.timerservice.config;

//简单工厂模式
public class ConfigFactory {
    public static Config getConfigObject(String method) {
        if (
                (method.equalsIgnoreCase("xysec-prod")) ||
                        (method.equalsIgnoreCase("xysec-dev"))
        ) {
            return new ParseConfigByParameters();
        } else if (method.equals("xysec-dev-local")) {
            return new XysecParseConfigByLocalFile();
        } else {
            return new ParseConfigByLocalFile();
        }
    }
}
