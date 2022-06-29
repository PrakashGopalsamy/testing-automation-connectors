package com.infosys.test.automation.connectors;

import java.util.List;

public abstract class Connector {
    public abstract List<String> getData() throws Exception;
}
