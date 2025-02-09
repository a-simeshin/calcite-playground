package com.github.asimeshin.service;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.calcite.schema.Schema;

import java.lang.reflect.Field;
import java.util.List;

@UtilityClass
public class SparkSqlUtils {

    @SneakyThrows
    public static void setSchema(org.apache.calcite.jdbc.CalciteConnection calciteConnection,
                                 String schemaName,
                                 SparkSqlService.SparkSqlSchema schema) {
        // Создаем новую SQL схему данных с помощью Reflect механизма
        final org.apache.calcite.adapter.java.ReflectiveSchema newSchema =
                new org.apache.calcite.adapter.java.ReflectiveSchema(schema);

        // Получаем корневую схему данных
        final org.apache.calcite.schema.SchemaPlus rootSchemaPlus = calciteConnection.getRootSchema();
        // Получаем сабсхему пользовательского уровня, где выполняется SQL
        final org.apache.calcite.schema.SchemaPlus oldSubSchemaPlus = rootSchemaPlus.getSubSchema(schemaName);

        // Если сабсхема пользовательского уровня не установлена, то ее можно и нужно установить первый раз
        // используя стандартные способоы Apache Calcite
        if (oldSubSchemaPlus == null) {
            rootSchemaPlus.add(schemaName, newSchema);
            calciteConnection.setSchema(schemaName);
            return;
        }

        // Если сабсхема пользовательского уровня уже была установлена, изменить на нее ссылку уже не так просто.
        // Гораздо проще заменить данные из старой схемы на данные из новой схемы
        final Schema oldSchema = getReflectiveSchemaFromSubSchema(oldSubSchemaPlus);
        replaceSchemaData(newSchema, oldSchema);
        calciteConnection.setSchema(schemaName);
    }

    @SneakyThrows
    private Schema getReflectiveSchemaFromSubSchema(org.apache.calcite.schema.Schema subSchema) {
        final Field wrapperField = subSchema.getClass().getField("this$0");
        wrapperField.setAccessible(true);
        final Object thisSchemaWrapper = wrapperField.get(subSchema);
        final Field schemaField = thisSchemaWrapper.getClass().getField("schema");
        schemaField.setAccessible(true);
        return (Schema) schemaField.get(thisSchemaWrapper);
    }

    static final List<String> REPLACE_FIELDS = List.of("clazz", "target", "tableMap");

    @SneakyThrows
    private void replaceSchemaData(org.apache.calcite.schema.Schema from, org.apache.calcite.schema.Schema to) {
        for (String fieldName: REPLACE_FIELDS) {
            final Field field = from.getClass().getField(fieldName);
            field.setAccessible(true);
            field.set(to, field.get(from));
        }
    }
}
