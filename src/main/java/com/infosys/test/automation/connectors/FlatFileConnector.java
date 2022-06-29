package com.infosys.test.automation.connectors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.infosys.test.automation.dto.CondElement;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FlatFileConnector extends Connector implements ConnectorProvider{

    private final String PROVIDER_NAME="file";

    public FlatFileConnector(String sourceName,Properties connectorProperties,
                             List<String> parentRecords, CondElement filterCond,
                             CondElement joinCond){
        super(sourceName,connectorProperties,parentRecords,filterCond,joinCond);
    }

    protected List<String> read() throws Exception{
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
