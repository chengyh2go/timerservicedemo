package com.xysec.timerservice.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class ParseConfigByParameters implements Config{
    @Override
    public Properties getConfig(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);

        //configFile为默认名称
        String configFilePath = params.get("configFile");
        Properties properties = new Properties();
        try {
            properties.load(new StringReader(new String(Files.readAllBytes(Paths.get(configFilePath)), StandardCharsets.UTF_8)));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
