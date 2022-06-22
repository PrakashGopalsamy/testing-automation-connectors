package com.infosys.test.automation.connectors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.json.simple.JSONObject;

public class FlatFileConnector extends Connector implements ConnectorProvider{

    private final String PROVIDER_NAME="hive";

    private Properties connectorProperties;

    public FlatFileConnector(Properties connectorProperties){
        this.connectorProperties = connectorProperties;
    }

    @Override
    public List<String> read() throws Exception{
        List<String> data = new ArrayList<>();
        String fileName = connectorProperties.getProperty("filename");
        String[] columns = connectorProperties.getProperty("sourcecolumns").split(",");
        String delimiter = connectorProperties.getProperty("columndelimiter");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
        String line = bufferedReader.readLine();
        while(line != null){
            JSONObject jsonObject = new JSONObject();
            String [] columnValues = line.split(delimiter);
            for(int i=0;i < columnValues.length;i++){
                jsonObject.put(columns[i],columnValues[i]);
            }
            data.add(jsonObject.toJSONString());
            line = bufferedReader.readLine();
        }
        return data;
    }

    @Override
    public String providerName() {
        return PROVIDER_NAME;
    }

}
