package com.umxwe.elasticsearch.plugin.distance;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName MlutArrayValuesSource
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/24
 */
public class MultArrayValuesSource<VS extends ValuesSource,VD extends ValuesSource> {
    protected MultiValueMode multiValueMode;
    protected String[] names;
    protected VS[]  values1;
    protected VD[]  values2;

    public static class MapArrayValuesSource extends MultArrayValuesSource<ValuesSource.Numeric,ValuesSource.GeoPoint> {

        public MapArrayValuesSource(Map<String, ValuesSource> valuesSources, MultiValueMode multiValueMode) {
            super(valuesSources, multiValueMode);
            if (valuesSources != null) {
                for (Map.Entry<String, ValuesSource> item:valuesSources.entrySet()
                     ) {

                    if(item.getValue() instanceof ValuesSource.Numeric == true){
                        this.values1=valuesSources.values().toArray(new ValuesSource.Numeric[0]);
                    }
                    else if(item.getValue() instanceof ValuesSource.GeoPoint == true){
                        this.values2=valuesSources.values().toArray(new ValuesSource.GeoPoint[0]);
                    }
                }

            } else {
                this.values1 = new ValuesSource.Numeric[0];
                this.values2 = new ValuesSource.GeoPoint[0];
            }
        }

        public Tuple<NumericDoubleValues, MultiGeoPointValues> getField(final int ordinal, LeafReaderContext ctx) throws IOException {
            if (ordinal > names.length) {
                throw new IndexOutOfBoundsException("ValuesSource array index " + ordinal + " out of bounds");
            }
            return new Tuple<>(multiValueMode.select(values1[ordinal].doubleValues(ctx)),values2[ordinal].geoPointValues(ctx));
        }
    }
    private MultArrayValuesSource(Map<String, ?> valuesSources, MultiValueMode multiValueMode) {
        if (valuesSources != null) {
            this.names = valuesSources.keySet().toArray(new String[0]);
        }
        this.multiValueMode = multiValueMode;
    }


    public boolean needsScores() {
        boolean needsScores1 = false;
        boolean needsScores2 = false;
        for (ValuesSource value : values1) {
            needsScores1 |= value.needsScores();
        }
        for (ValuesSource value : values2) {
            needsScores2 |= value.needsScores();
        }
        return needsScores1 | needsScores2;
    }

    public String[] fieldNames() {
        return this.names;
    }

}
