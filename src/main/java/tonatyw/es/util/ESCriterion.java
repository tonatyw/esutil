package tonatyw.es.util;

import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;

public interface ESCriterion {
    public enum Operator {
        TERM, TERMS, RANGE, FUZZY, QUERY_STRING, MISSING, MATCH_PHRASE, MATCH
    }

    public enum MatchMode {
        START, END, ANYWHERE
    }

    public enum Projection {
        MAX, MIN, AVG, LENGTH, SUM, COUNT
    }

    public List<QueryBuilder> listBuilders();
}
