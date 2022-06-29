package com.infosys.test.automation.connectors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import com.infosys.test.automation.dto.CondElement;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

public class FlatFileConnector extends Connector implements ConnectorProvider{

    private final String PROVIDER_NAME="file";

    private Properties connectorProperties;

    private List<String> parentRecords;

    private CondElement filterCond;

    private CondElement joinCond;

    public FlatFileConnector(Properties connectorProperties){
        this.connectorProperties = connectorProperties;
    }

    @Override
    public List<String> getData() throws Exception{
        List<String> sourceData = read();
        List<String> filteredData = applyFilter(sourceData);
        return filteredData;
    }

    private List<String> read() throws Exception{
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

    private List<String> applyFilter(List<String> childRecords){
        if (filterCond != null){
            if (parentRecords != null && parentRecords.size() > 0){
                List<String> filteredChildRecords = childRecords.stream().filter(
                        childRecord -> parentRecords.stream().anyMatch(parentRecord -> {
                            try {
                                return filterCond.evaluateCondition(parentRecord,childRecord);
                            } catch (ParseException e) {
                                e.printStackTrace();
                                return false;
                            }
                        })
                ).collect(Collectors.toList());
                return filteredChildRecords;
            }
            else{
                List<String> filteredChildRecords = childRecords.stream().filter(childRecord -> {
                    try {
                        return filterCond.evaluateCondition(null,childRecord);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        return false;
                    }
                }).collect(Collectors.toList());
                return filteredChildRecords;
            }
        } else{
            return childRecords;
        }
    }

    @Override
    public String providerName() {
        return PROVIDER_NAME;
    }

}
