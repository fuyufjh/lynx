package me.ericfu.lightning.source.text;

import com.google.common.base.Preconditions;
import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.BasicType;
import me.ericfu.lightning.schema.Field;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.schema.RecordTypeBuilder;
import me.ericfu.lightning.source.SchemalessSource;
import me.ericfu.lightning.source.SourceReader;

public class TextSource implements SchemalessSource {

    final GeneralConf globals;
    final TextSourceConf conf;

    byte sep;
    RecordType schema;

    public TextSource(GeneralConf globals, TextSourceConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    public void provideSchema(RecordType schema) {
        RecordTypeBuilder builder = new RecordTypeBuilder();
        for (Field field : schema.getFields()) {
            // Always provide strings regardless of the requested type
            builder.addField(field.getName(), BasicType.STRING);
        }
        this.schema = builder.build();
    }

    @Override
    public RecordType getSchema() {
        Preconditions.checkState(schema != null);
        return schema;
    }

    @Override
    public void init() throws DataSourceException {
        // do nothing
    }

    @Override
    public SourceReader createReader(int partNo) {
        return new TextSourceReader(this, partNo);
    }
}


