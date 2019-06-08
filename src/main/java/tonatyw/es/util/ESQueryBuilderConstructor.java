package tonatyw.es.util;
import org.elasticsearch.index.query.BoolQueryBuilder;  
import org.elasticsearch.index.query.QueryBuilder;  
import org.elasticsearch.index.query.QueryBuilders;  
  
import java.util.ArrayList;  
import java.util.List;  

public class ESQueryBuilderConstructor {  
  
    private int size = Integer.MAX_VALUE;  
  
    private int from = 0;  
  
    private String asc;  
  
    private String desc;  
    
    private ESGroupby esGroupby;
  
    private List<ESCriterion> mustCriterions = new ArrayList<ESCriterion>();  
    private List<ESCriterion> shouldCriterions = new ArrayList<ESCriterion>();
    private List<ESCriterion> mustNotCriterions = new ArrayList<ESCriterion>();  
   
    public QueryBuilder listBuilders() {  
        int count = mustCriterions.size() + shouldCriterions.size() + mustNotCriterions.size();  
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();  
        QueryBuilder queryBuilder = null;  
  
        if (count >= 1) {  
            //must匹配  
            if (mustCriterions!=null && mustCriterions.size()>0) {  
                for (ESCriterion criterion : mustCriterions) {  
                    for (QueryBuilder builder : criterion.listBuilders()) {  
                        queryBuilder = boolQueryBuilder.must(builder);  
                    }  
                }  
            }  
            //should匹配
            if (shouldCriterions!=null && shouldCriterions.size()>0) {  
                for (ESCriterion criterion : shouldCriterions) {  
                    for (QueryBuilder builder : criterion.listBuilders()) {  
                        queryBuilder = boolQueryBuilder.should(builder);  
                    }  
  
                }  
            }  
            //must not   
            if (mustNotCriterions!=null && mustNotCriterions.size()>0) {  
                for (ESCriterion criterion : mustNotCriterions) {  
                    for (QueryBuilder builder : criterion.listBuilders()) {  
                        queryBuilder = boolQueryBuilder.mustNot(builder);  
                    }  
                }  
            }  
            return queryBuilder;  
        } else {  
            return null;  
        }  
    }  
  
    /**  
     * 精确匹配
     */  
    public ESQueryBuilderConstructor must(ESCriterion criterion){  
        if(criterion!=null){  
            mustCriterions.add(criterion);  
        }  
        return this;  
    }  
    /** 
     * 模糊匹配
     */  
    public ESQueryBuilderConstructor should(ESCriterion criterion){  
        if(criterion!=null){  
            shouldCriterions.add(criterion);  
        }  
        return this;  
    }  
    /** 
     * 不匹配
     */  
    public ESQueryBuilderConstructor mustNot(ESCriterion criterion){  
        if(criterion!=null){  
            mustNotCriterions.add(criterion);  
        }  
        return this;  
    }  
  
  
    public int getSize() {  
        return size;  
    }  
  
    public void setSize(int size) {  
        this.size = size;  
    }  
  
    public String getAsc() {  
        return asc;  
    }  
  
    public void setAsc(String asc) {  
        this.asc = asc;  
    }  
  
    public String getDesc() {  
        return desc;  
    }  
  
    public void setDesc(String desc) {  
        this.desc = desc;  
    }  
  
    public int getFrom() {  
        return from;  
    }  
  
    public void setFrom(int from) {  
        this.from = from;  
    }

	public ESGroupby getEsGroupby() {
		return esGroupby;
	}

	public void setEsGroupby(ESGroupby esGroupby) {
		this.esGroupby = esGroupby;
	}
    
}  