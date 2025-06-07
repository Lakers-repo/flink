package org.apache.flink.table.examples.java.functions;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;

import org.apache.flink.table.examples.java.types.BitmapValue;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

import java.util.Optional;

@Slf4j
public class BitMapCountWithRetract extends AggregateFunction<Long, BitmapValue> {

    @Override
    public BitmapValue createAccumulator() {
        return new BitmapValue();
    }

    @Override
    public Long getValue(BitmapValue acc) {
        return acc == null ? 0L : acc.cardinality();
    }

    public void accumulate(BitmapValue acc, Long iValue) {
        if (iValue != null) {
            acc.add(iValue);
        }
    }

    public void retract(BitmapValue acc, Long iValue) throws Exception {
//        if (acc.serializeToString() == null || acc.serializeToString().equals("")) {
//            return;
//        }
        if (iValue != null) {
            log.info("before retract element:{}, count:{}, retracted value:{}", acc.serializeToString(),acc.cardinality(),iValue);
            // only xor when the pre acc value is not blank
//            if (StringUtils.isNotBlank(acc.serializeToString())) {
                BitmapValue retractValue = new BitmapValue();
                retractValue.add(iValue);
                acc.xor(retractValue);
//            }
            log.info("after retract element:{}, count:{}", acc.serializeToString(),acc.cardinality());
        }
    }

    public void merge(BitmapValue acc, Iterable<BitmapValue> bitmapValues) {

        for (BitmapValue bitmapValue : bitmapValues) {
            if (bitmapValue != null) {
                switch (bitmapValue.getBitmapType()) {
                    case 0:
                        break;
                    case 1:
                        acc.add(bitmapValue.getSingleValue());
                        break;
                    case 2:
                        bitmapValue.getBitmap().forEach(acc::add);
                        break;
                    case 3:
                        for (Long aLong : bitmapValue.getSet()) {
                            acc.add(aLong);
                        }
                        break;
                }
            }
        }
    }

    public void resetAccumulator(BitmapValue acc) {
        acc.clear();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(callContext -> Optional.of(DataTypes.BIGINT()))
                .accumulatorTypeStrategy(callContext -> Optional.of(TypeInfoDataTypeConverter.toDataType(typeFactory,
                        Types.POJO(BitmapValue.class)))).build();
    }
}
