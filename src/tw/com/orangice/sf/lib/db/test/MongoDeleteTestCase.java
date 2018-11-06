package tw.com.orangice.sf.lib.db.test;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import tw.com.orangice.sf.lib.db.MongoDatabaseManager;
import tw.com.orangice.sf.lib.db.component.Criteria;
import tw.com.orangice.sf.lib.db.component.CriteriaCompo;
import tw.com.orangice.sf.lib.log.LogService;

public class MongoDeleteTestCase {
	static MongoDatabaseManager mongoMgr = null;
	public MongoDeleteTestCase() {
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		LogService logger = null;
		mongoMgr = new MongoDatabaseManager(logger, "localhost", 27017, "test-java-collection");
		mongoMgr.dropCollection("test-java-table");
		mongoMgr.insertSQL("test-java-table", new String[]{"aaa","bbb"}, new String[]{"1","2"}, "code");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		try{
			CriteriaCompo condition = new CriteriaCompo(new Criteria("aaa", "1"));
			mongoMgr.deleteSQL("test-java-table", condition);
			assert(true);
		}
		catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
