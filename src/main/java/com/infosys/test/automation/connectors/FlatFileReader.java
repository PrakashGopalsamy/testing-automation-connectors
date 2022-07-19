package com.infosys.test.automation.connectors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.infosys.test.automation.connectors.exceptions.InvalidCnnctPropException;
import com.infosys.test.automation.constants.ConnectorConstants;
import com.infosys.test.automation.dto.CondConfig;
import org.json.simple.JSONObject;

public class FlatFileReader extends DataReader implements ReaderProvider {

    private final String READER_NAME ="flat_file_reader";

    public FlatFileReader(){

    }

    public FlatFileReader(String sourceName, Properties connectorProperties,
                          List<String> parentRecords, CondConfig filterConfig,
                          CondConfig joinConfig) throws InvalidCnnctPropException {
        super(sourceName,connectorProperties,parentRecords,filterConfig,joinConfig);
    }

    protected List<String> read() throws Exception{
        List<String> data = new ArrayList<>();
        String fileName = connectorProperties.getProperty(ConnectorConstants.FILENAME);
        String[] columns = connectorProperties.getProperty(ConnectorConstants.SOURCECOLUMNS).split(",");
        String delimiter = connectorProperties.getProperty(ConnectorConstants.COLUMNDELIMITER);
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
    protected boolean validatePropeties() throws InvalidCnnctPropException {
        boolean validProps = true;
        String fileName = connectorProperties.getProperty(ConnectorConstants.FILENAME);
        String columns = connectorProperties.getProperty(ConnectorConstants.SOURCECOLUMNS);
        String delimiter = connectorProperties.getProperty(ConnectorConstants.COLUMNDELIMITER);
        StringBuilder exceptionMessageBuilder = new StringBuilder();
        if (fileName == null || fileName.trim().length() == 0){
            exceptionMessageBuilder.append("The required property "+ConnectorConstants.FILENAME+" is not been provided\n");
            validProps=false;
        }
        if (columns == null || columns.trim().length() == 0){
            exceptionMessageBuilder.append("The required property "+ConnectorConstants.SOURCECOLUMNS+" is not been provided\n");
            validProps=false;
        }
        if (delimiter == null || delimiter.trim().length() == 0){
            exceptionMessageBuilder.append("The required property "+ConnectorConstants.COLUMNDELIMITER+" is not been provided\n");
            validProps=false;
        }
        if (!validProps){
            throw new InvalidCnnctPropException(exceptionMessageBuilder.toString());
        }
        return validProps;
    }

    @Override
    public String readerName() {
        return READER_NAME;
    }

}
