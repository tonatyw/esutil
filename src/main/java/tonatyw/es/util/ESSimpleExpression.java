package tonatyw.es.util;

import java.util.Collection;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import tonatyw.es.util.ESCriterion.Operator;


/***
 * 
 * 
 * @author wish30_1
 * @version $Id: ESSimpleExpression.java, v 0.1 2018年4月19日 上午11:01:52 wish30_1 Exp $
 */
public class ESSimpleExpression {
    private String               fieldName;
    private Object               value;
    private Collection<Object>   values;
    private ESCriterion.Operator operator;
    private Object               from;
    private Object               to;

    protected ESSimpleExpression(String fieldName, Object value, Operator operator) {
        this.fieldName = fieldName;
        this.value = value;
        this.operator = operator;
    }

    protected ESSimpleExpression(String value, Operator operator) {
        this.value = value;
        this.operator = operator;
    }

    protected ESSimpleExpression(String fieldName, Collection<Object> values) {
        this.fieldName = fieldName;
        this.values = values;
        this.operator = Operator.TERMS;
    }

    protected ESSimpleExpression(String fieldName, Object from, Object to) {
        this.fieldName = fieldName;
        this.from = from;
        this.to = to;
        this.operator = Operator.RANGE;
    }

    public QueryBuilder toBuilder() {
        QueryBuilder qb = null;
        switch (operator) {
            case TERM:
                qb = QueryBuilders.termQuery(fieldName, value);
                break;
            case TERMS:
                qb = QueryBuilders.termsQuery(fieldName, values);
                break;
            case RANGE:
                if(from == null){
                    qb = QueryBuilders.rangeQuery(fieldName).to(to).includeLower(true)
                            .includeUpper(true);
                }else if(to == null){
                    qb = QueryBuilders.rangeQuery(fieldName).from(from).includeLower(true)
                            .includeUpper(true);
                }else{
                    qb = QueryBuilders.rangeQuery(fieldName).from(from).to(to).includeLower(true)
                            .includeUpper(true);
                }
                break;
            case FUZZY:
                qb = QueryBuilders.fuzzyQuery(fieldName, value);
                break;
            case QUERY_STRING:
                qb = QueryBuilders.queryStringQuery(value.toString());
                break;
            case MATCH_PHRASE:
                qb = QueryBuilders.matchPhraseQuery(fieldName, value);
                break;
            case MATCH:
                qb = QueryBuilders.matchQuery(fieldName, value);
        }
        return qb;
    }
}