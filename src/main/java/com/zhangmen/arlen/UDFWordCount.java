package com.zhangmen.arlen;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UDFWordCount extends TableFunction<Row> {


    /*设置返回类型*/
    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.INT);
    }

    /*消息处理*/
    public void eval(String line){
        String[] wordSplit=line.split(",");
        for(int i=0;i<wordSplit.length;i++){
            Row row = new Row(2);
            row.setField(0, wordSplit[i]);
            row.setField(1, 1);
            collect(row);
        }
    }
}
