package tonatyw.es.util;

import java.lang.reflect.Method;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

/***
 * elasticsearch分组公用类
 * 
 * @author 廖莲
 * @version $Id: ESGroupby.java, v 0.1 2018年4月19日 上午10:53:51 wish30_1 Exp $
 */
public class ESGroupby {
    public interface SearchType{
        public static final int SEARCH_TERM = 1;
        public static final int SEARCH_DATE = 2;
    }
    public interface OrderBy{
        public static final int BY_KEY = 2;
        public static final int BY_COUNT = 1;
    }
    public interface DateUnix{
        public static final int YEAR = 29030400;
        public static final int MONTH = 2419200;
        public static final int WEEK = 604800;
        public static final int DAY = 86400;
        public static final int HOUR = 3600;
        public static final int MIN = 60;
        public static final int S = 1;
    }
    private String  field;
    private String  asName;
    private String  statsField;
    private String  statsAsName;
    private String  statsMethod;
    private String  dateFormat = "yyyy-MM-dd";
    private boolean order        = false;
    private boolean orderByStats = false;
    private int     from         = 1;
    private int     size         = 10;
    private int orderWay = 1;
    private int searchWay = 1;
    private DateHistogramInterval interval = DateHistogramInterval.DAY;

    public ESGroupby(String field, String asName, int from) {
        this.field = field;
        this.asName = asName;
        this.from = from;
    }

    public ESGroupby(String field, String asName) {
        this.field = field;
        this.asName = asName;
    }

    public ESGroupby(String field, String asName, int from, int size) {
        this.field = field;
        this.asName = asName;
        this.from = from;
        this.size = size;
    }

    public ESGroupby(String field) {
        this.field = field;
        this.asName = field;
    }

    public void max(String statsField, String asName) {
        this.statsField = statsField;
        this.statsMethod = "Max";
        this.statsAsName = asName;
    }

    public void min(String statsField, String asName) {
        this.statsField = statsField;
        this.statsMethod = "Min";
        this.statsAsName = asName;
    }

    public void sum(String statsField, String asName) {
        this.statsField = statsField;
        this.statsMethod = "Sum";
        this.statsAsName = asName;
    }

    public void avg(String statsField, String asName) {
        this.statsField = statsField;
        this.statsMethod = "Avg";
        this.statsAsName = asName;
    }

    public void esc() {
        this.order = true;
    }

    public void desc() {
        this.order = false;
    }
    
    public void asc(int by) {
        this.order = true;
        this.orderWay = by;
    }

    public void desc(int by) {
        this.order = false;
        this.orderWay = by;
    }

    public void orderByStats(boolean orderByStats) {
        this.orderByStats = orderByStats;
    }

    public String getKeyName() {
        if (orderByStats) {
            return statsAsName;
        } else {
            return asName;
        }
    }

    public ValuesSourceAggregationBuilder aggregationBuilder() {
        ValuesSourceAggregationBuilder tab = null;
        BucketOrder bo = null;
        if (orderByStats) {
            tab = AggregationBuilders.terms(asName).field(field).size(0);
        } else if(searchWay==SearchType.SEARCH_TERM){
            tab = AggregationBuilders.terms(asName).field(field).size(Integer.MAX_VALUE);
        } else if(searchWay==SearchType.SEARCH_DATE){
            tab = AggregationBuilders.dateHistogram(asName).field(field).format(dateFormat).dateHistogramInterval(interval).minDocCount(1);
        } else{
            try {
                throw new Exception("请通过esc或desc指定排序方式");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // 坑爹的esT T order方法竟然不在父类声明，非要在每个子类写一遍。
        if(searchWay == SearchType.SEARCH_TERM){
            if(orderWay==OrderBy.BY_COUNT){
                ((TermsAggregationBuilder)tab).order(BucketOrder.count(order));
            }else if(orderWay==OrderBy.BY_KEY){
                ((TermsAggregationBuilder)tab).order(BucketOrder.key(order));
            }
        }else if(searchWay == SearchType.SEARCH_DATE){
            if(orderWay==OrderBy.BY_COUNT){
                ((DateHistogramAggregationBuilder)tab).order(BucketOrder.count(order));
            }else if(orderWay==OrderBy.BY_KEY){
                ((DateHistogramAggregationBuilder)tab).order(BucketOrder.key(order));
            }
        }
        Object stats = null;
        if (StringUtils.isNotEmpty(statsMethod)) {
            stats = executeMethod(AggregationBuilders.class, statsMethod.toLowerCase(),
                new Class[] { String.class }, statsAsName);
            ValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Numeric, MinAggregationBuilder> ss = (ValuesSourceAggregationBuilder.LeafOnly<ValuesSource.Numeric, MinAggregationBuilder>) stats;
            ss.field(statsField);
            tab.subAggregation((AggregationBuilder) ss);
        }
        return tab;
    }

    private Object executeMethod(Class c, String methodName, Class[] classes, Object... args) {
        Object obj = null;
        try {
            Method m = c.getDeclaredMethod(methodName, classes);
            obj = m.invoke(c, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return obj;
    }
    public void setSearchWay(int searchWay,String format,DateHistogramInterval interval){
        this.searchWay = searchWay;
        this.dateFormat = format;
        this.interval = interval;
    }
    public static void main(String[] args) {
        ESGroupby esg = new ESGroupby("1");
        Object o = esg.executeMethod(Integer.class, "max", new Class[] { int.class, int.class }, 5,
            7);
        System.out.println(o);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getAsName() {
        return asName;
    }

    public void setAsName(String asName) {
        this.asName = asName;
    }

    public String getStatsField() {
        return statsField;
    }

    public void setStatsField(String statsField) {
        this.statsField = statsField;
    }

    public String getStatsAsName() {
        return statsAsName;
    }

    public void setStatsAsName(String statsAsName) {
        this.statsAsName = statsAsName;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public boolean isOrderByStats() {
        return orderByStats;
    }

    public void setOrderByStats(boolean orderByStats) {
        this.orderByStats = orderByStats;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getStatsMethod() {
        return statsMethod;
    }

    public void setStatsMethod(String statsMethod) {
        this.statsMethod = statsMethod;
    }
    
    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }
}
