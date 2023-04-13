package utils;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class CsvSchema extends AbstractSchema {
    private Map<String, Table> tableMap = new HashMap<>();

    private String dataFiles;

    public CsvSchema(String dataFile) {
        this.dataFiles = dataFile;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        for (String dataFile : dataFiles.split(",")) {
            URL url = ClassLoader.getSystemClassLoader().getResource(dataFile);
            assert url != null;
            Source source = Sources.of(url);
            tableMap.put(dataFile.split("\\.")[0].toUpperCase(), new CsvTable(source));
        }
        return tableMap;
    }
}