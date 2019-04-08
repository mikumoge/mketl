package com.mycat.mketl.core;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Configuration {

    private static Configuration config=null;
    private static Properties resourceData=new Properties();
    private static String commonReg=null;
    private static Logger LOG=Logger.getLogger(Configuration.class);

    private Configuration(String resourceFile){
        loadResourceData(resourceFile);
    }

    private void loadResourceData(String resourceFile) {
        try {
            LOG.info("Begin to parse job configuration.");
            resourceData.load(getClass().getClassLoader().getResourceAsStream(resourceFile));
            LOG.info("Parsed job configuration finished..");
        } catch (IOException e) {
            LOG.error("Load conf failed!",new FileNotFoundException("The goal resource file "+resourceFile+" not found."));
        }
    }

    public static Configuration getInstance(String conf){
        LOG.info("Begin to load default configuration.");
        loadDefaultConfig();
        config=(config==null)?new Configuration(conf):config;
        commonReg=resourceData.getProperty("common.regex");
        return config;
    }

    private static void loadDefaultConfig() {
        try {
            resourceData.load(Configuration.class.getClassLoader().getResourceAsStream("default.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public String get(String key){
        return resourceData.getProperty(key);
    }

    public Map<String,String> getConfBeginWith(String prefix){
        Map<String,String> result=new HashMap<>();
        getKeyIterator().forEachRemaining(cf -> {
            if(cf.startsWith(prefix)) result.put(cf,resourceData.getProperty(cf));
        });
        return result;
    }

    public List<String> getBeginWith(String prefix){
        List<String> result=new ArrayList<>();
        getKeyIterator().forEachRemaining(cf -> {
            if(cf.startsWith(prefix)) result.add(resourceData.getProperty(cf));
        });
        return result;
    }

    public Iterator<String> getKeyIterator(){
        return resourceData.stringPropertyNames().iterator();
    }

    public static Configuration getConfig(){
        return config;
    }


    public void set(String configKey,String configValue){
        resourceData.setProperty(configKey,configValue);
    }

    public Map<String,String> getConnectionConfig(){
        Map<String,String> result=new HashMap<>();
        String cnnReg=commonReg+"cnns$";
        getKeyIterator().forEachRemaining(cf -> {
            if(cf.matches(cnnReg))result.put(getKey(cf,cnnReg),resourceData.getProperty(cf));
        });
        return result;
    }
    public String getKey(String cf,String cnnReg){
        Pattern pattern = Pattern.compile(cnnReg);
        Matcher m = pattern.matcher(cf);
        if(m.matches()){
            return m.group(1);
        }
        return null;
    }

    public List<String> getByRegex(String regex){
        List<String> result=new ArrayList<>();
        getKeyIterator().forEachRemaining(cf -> {
            if(cf.matches(regex)) result.add(resourceData.getProperty(cf));
        });
        return result;
    }

    public String getCommonConfig(String cid,String endfix){
        AtomicReference<String> result=new AtomicReference<>();
        getKeyIterator().forEachRemaining(cf -> {
            if(cf.equals(cid+"."+endfix)) result.set(resourceData.getProperty(cf));
        });
        return result.get();
    }
}
