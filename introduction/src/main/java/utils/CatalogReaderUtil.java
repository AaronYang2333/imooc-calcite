package utils;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;

import java.net.URISyntaxException;
import java.util.Properties;

public class CatalogReaderUtil {
    public static CalciteCatalogReader createCatalogReader(SqlParser.Config parserConfig) throws URISyntaxException {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("csv",new CsvSchema("data.csv"));
//        rootSchema.add("csv",new CsvSchema("dataA.csv"));
//        rootSchema.add("csv",new CsvSchema("dataB.csv"));
        return createCatalogReader(parserConfig, rootSchema);
    }

    public static CalciteCatalogReader createCatalogReader(SqlParser.Config parserConfig, SchemaPlus rootSchema) {

        Properties prop = new Properties();
        prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                Boolean.TRUE.toString());
        CalciteConnectionConfigImpl calciteConnectionConfig = new CalciteConnectionConfigImpl(prop);
        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(rootSchema).path("csv"),
                new JavaTypeFactoryImpl(),
                calciteConnectionConfig
        );
    }

}
