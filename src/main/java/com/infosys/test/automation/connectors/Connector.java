package com.infosys.test.automation.connectors;

import com.infosys.test.automation.dto.CondElement;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public abstract class Connector {

    protected String sourceName;

    protected Properties connectorProperties;

    protected List<String> parentRecords;

    protected CondElement filterCond;

    protected CondElement joinCond;

    public Connector(){

    }

    public Connector(String sourceName,Properties connectorProperties,
                     List<String> parentRecords, CondElement filterCond,
                     CondElement joinCond){
        this.sourceName = sourceName;
        this.connectorProperties = connectorProperties;
        this.parentRecords = parentRecords;
        this.filterCond = filterCond;
        this.joinCond = joinCond;
    }


    public List<String> getData() throws Exception{
        List<String> sourceData = read();
        List<String> filteredData = applyFilter(sourceData);
//        for (String fltrDt: filteredData){
//            System.out.println("Filtered Data : "+fltrDt);
//        }
        List<String> framedResult = frameResult(filteredData);
        return framedResult;
    }

    protected abstract List<String> read() throws Exception;

    protected List<String> applyFilter(List<String> childRecords){
        List<String> filteredRecords = null;
        if (filterCond != null){
            if (parentRecords != null && parentRecords.size() > 0){
                filteredRecords = childRecords.stream().filter(
                        childRecord -> parentRecords.stream().anyMatch(parentRecord -> {
                            try {
                                return filterCond.evaluateCondition(parentRecord,childRecord);
                            } catch (ParseException e) {
                                e.printStackTrace();
                                return false;
                            }
                        })
                ).collect(Collectors.toList());
            }
            else{
                filteredRecords = childRecords.stream().filter(childRecord -> {
                    try {
                        return filterCond.evaluateCondition(null,childRecord);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        return false;
                    }
                }).collect(Collectors.toList());
            }
        } else{
            filteredRecords = childRecords;
        }

        return filteredRecords;
    }

    protected List<String> frameResult(List<String> filteredChildRcrds){
        String[] columns = connectorProperties.getProperty("sourcecolumns").split(",");
        JSONParser jsonParser = new JSONParser();
        List<String> childRecords;
        List<String> framedResult;
        if (joinCond != null && parentRecords != null && parentRecords.size() > 0){
            childRecords = filteredChildRcrds.stream().filter(
                    childRecord -> parentRecords.stream().anyMatch(parentRecord -> {
//                        System.out.println("Parent Record : "+parentRecord);
                        try {
                            return joinCond.evaluateCondition(parentRecord,childRecord);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return false;
                        }
                    })
            ).collect(Collectors.toList());
            framedResult = parentRecords.stream().flatMap(parentRecord -> {
                List<String> joinedRecords = new ArrayList<>();
                try {
                    List<String> matchedChildRecords = childRecords.stream().filter(
                            childRecord -> {
                                try {
                                    return joinCond.evaluateCondition(parentRecord, childRecord);
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return false;
                                }
                            }).collect(Collectors.toList());
                    if (matchedChildRecords.size() > 0) {
                        for (String childRecord : matchedChildRecords) {
                            JSONObject parentObject = (JSONObject) jsonParser.parse(parentRecord);
                            JSONObject childObject = (JSONObject) jsonParser.parse(childRecord);
                            for (String column : columns) {
                                parentObject.put(sourceName + "." + column, childObject.get(column));
                            }
                            joinedRecords.add(parentObject.toJSONString());
                        }
                    } else {
                        joinedRecords.add(parentRecord);
                    }
                    return joinedRecords.stream();
                } catch (ParseException e) {
                    e.printStackTrace();
                    joinedRecords.add(parentRecord);
                    return joinedRecords.stream();
                }
            }).collect(Collectors.toList());
        } else{
            framedResult = filteredChildRcrds.stream().map(filteredRcrd -> {
                try {
                    JSONObject filteredObject = (JSONObject) jsonParser.parse(filteredRcrd);
                    JSONObject targetObject  = new JSONObject();
                    for (String column : columns) {
                        targetObject.put(sourceName + "." + column, filteredObject.get(column));
                    }
                    return targetObject.toJSONString();
                } catch (ParseException e) {
                    e.printStackTrace();
                    return "";
                }
            }).filter(rcrd -> rcrd.trim().length() > 0).collect(Collectors.toList());
        }
        return framedResult;
    }
}
