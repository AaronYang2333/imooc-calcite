package utils;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Source;
import org.apache.commons.lang3.time.FastDateFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class CsvEnumerator<E> implements Enumerator<E> {

    private E current;

    private BufferedReader br;

    private List<SqlTypeName> types;

    CsvEnumerator(Source source) {
        try {
            this.br = new BufferedReader(source.reader());
            String[] columnWithType = this.br.readLine().split(" ");
            types = new ArrayList<>(columnWithType.length);
            for (String str : columnWithType) {
                String type = str.split(":")[1];
                types.add(SqlTypeName.get(type));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public E current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try {
            String line = br.readLine();
            if (line == null || "".equals(line.trim())) {
                return false;
            }
            final String[] values = line.split(",");
            Object[] row = new Object[values.length];
            for (int i = 0; i < values.length; i++) {
                row[i] = convert(types.get(i), values[i]);
            }
            current = (E) row;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("Error");
    }

    @Override
    public void close() {
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static {
        final TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_DATE = FastDateFormat.getInstance("yyyy-MM-dd", gmt);
        TIME_FORMAT_TIME = FastDateFormat.getInstance("HH:mm:ss", gmt);
        TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }

    private Object convert(SqlTypeName fieldType, String string) {
        if (fieldType == null) {
            return string;
        }
        switch (fieldType) {
            case BOOLEAN:
                if (string.length() == 0) {
                    return null;
                }
                return Boolean.parseBoolean(string);
            case TINYINT:
                if (string.length() == 0) {
                    return null;
                }
                return Byte.parseByte(string);
            case SMALLINT:
                if (string.length() == 0) {
                    return null;
                }
                return Short.parseShort(string);
            case INTEGER:
                if (string.length() == 0) {
                    return null;
                }
                return Integer.parseInt(string);
            case BIGINT:
                if (string.length() == 0) {
                    return null;
                }
                return Long.parseLong(string);
            case FLOAT:
                if (string.length() == 0) {
                    return null;
                }
                return Float.parseFloat(string);
            case DOUBLE:
                if (string.length() == 0) {
                    return null;
                }
                return Double.parseDouble(string);
            case DATE:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_DATE.parse(string);
                    return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
                } catch (ParseException e) {
                    return null;
                }
            case TIME:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_TIME.parse(string);
                    return (int) date.getTime();
                } catch (ParseException e) {
                    return null;
                }
            case TIMESTAMP:
                if (string.length() == 0) {
                    return null;
                }
                try {
                    Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                    return date.getTime();
                } catch (ParseException e) {
                    return null;
                }
            case VARCHAR:
            default:
                return string;
        }
    }
}
