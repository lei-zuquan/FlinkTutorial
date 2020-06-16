package com.lei.apitest.c07_sql_api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:22 下午 2020/6/16
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
    UDTF 要继承 TableFunction
 */
public class C06_Split extends TableFunction<Row> {

    private String separator = ",";

    public C06_Split(String separator) {
        this.separator = separator;
    }

    public void eval(String line) {
        for (String s : line.split(separator)) {
            collect(Row.of(s));
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING);
    }
}
