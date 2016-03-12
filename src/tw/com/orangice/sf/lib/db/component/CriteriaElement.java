package tw.com.orangice.sf.lib.db.component;

public abstract class CriteriaElement {
    protected String order = "ASC";
    
    protected String sort = "";
   
    protected int limit = 0;

    protected int start = 0;

    protected String groupby = "";
   
    public abstract String render();
    
    public abstract String renderWhere();
    
    
    public void setSort(String sort)
    {
        this.sort = sort;
    }
    
}
