package fr.an.tools.jdbclogger.calllogger.templaterecorder;

import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import fr.an.tools.jdbclogger.calllogger.templaterecorder.TemplateSqlUtil;

public class TemplateSqlUtilTest extends Assert {

	@Test
	public void testSql() {
		String sql = "select count(1) from A a WHERE a.x = 123 AND (a.date < to_date('24-01-2018','dd-mm-yyyy') or a.y = 2)";
		String sqlTempl = TemplateSqlUtil.templatizeSqlText(sql);
		assertFalse(sql.equals(sqlTempl));

		String sql2 = "select count(1) from A a WHERE a.x = 234 AND (a.date < to_date('24-01-2019','dd-mm-yyyy') or a.y = 3)";
		String sqlTempl2 = TemplateSqlUtil.templatizeSqlText(sql2);
		assertEquals(sqlTempl2, sqlTempl);
	}
	
	@Test
	public void testComp() {
		Pattern re = Pattern.compile(TemplateSqlUtil.comparatorRE); 
		assertMatches("=", re);
	}

	private void assertMatches(String text, Pattern re) {
		boolean matches = re.matcher(text).matches();
		assertTrue("regexp does not match text: text:'" + text + "',  regexp:'" + re + "'", matches);
	}
	
	@Test
	public void testSqlInsertFromSelectIn() {
		String sql = "insert into A select b.col1 from B b where b.col2 in (select e.col1 from E e where e.a = 1)";
		String sqlTempl = TemplateSqlUtil.templatizeSqlText(sql);
		assertFalse(sql.equals(sqlTempl));

		String sql2 = "insert into A select b.col1 from B b where b.col2 in (select e.col1 from E e where e.a = 2)";
		String sqlTempl2 = TemplateSqlUtil.templatizeSqlText(sql2);
		assertEquals(sqlTempl2, sqlTempl);
	}

	@Test
	public void templatizeSqlText_InsertInto() {
		String sql = "insert into A(col1, col2) VALUES (1, 'A')";
		String sqlTempl = TemplateSqlUtil.templatizeSqlText(sql);
		assertFalse(sql.equals(sqlTempl));

		String sql2 = "insert into A(col1, col2) VALUES (2, 'B')";
		String sqlTempl2 = TemplateSqlUtil.templatizeSqlText(sql2);
		assertEquals(sqlTempl2, sqlTempl);
	}

	public void testInsertIntoSelect() {
		String sql = "INSERT INTO A (col1, col2) SELECT 1, 'a' FROM DUAL";
		String sqlTempl = TemplateSqlUtil.templatizeSqlText(sql);
		assertFalse(sql.equals(sqlTempl));

		String sql2 = "INSERT INTO A (col1, col2) SELECT 2, 'b' FROM DUAL";
		String sqlTempl2 = TemplateSqlUtil.templatizeSqlText(sql2);
		assertEquals(sqlTempl2, sqlTempl);
	}
	
	@Test
	public void testSelectFromSelect() {
		String sql = "SELECT * FROM (SELECT a.* from A a where a.col1 = 2) order by a.b";
		String sqlTempl = TemplateSqlUtil.templatizeSqlText(sql);
		assertFalse(sql.equals(sqlTempl));

		String sql2 = "SELECT * FROM (SELECT a.* from A a where a.col1 = 2) order by a.b";
		String sqlTempl2 = TemplateSqlUtil.templatizeSqlText(sql2);
		assertEquals(sqlTempl2, sqlTempl);
	}
	
	@Test
	public void testProc() {
		String sql = "Begin callProc(1234); End";
		String sqlTempl = TemplateSqlUtil.templatizeSqlText(sql);
		assertFalse(sql.equals(sqlTempl));
	}

	@Test
	public void testProc2() {
		String sql = "Begin callProc('abc'); End";
		String sqlTempl = TemplateSqlUtil.templatizeSqlText(sql);
		assertFalse(sql.equals(sqlTempl));
	}

	@Test
	public void testProc3() {
		String sql = "Begin callProc('abc', 123); End";
		String sqlTempl = TemplateSqlUtil.templatizeSqlText(sql);
		assertFalse(sql.equals(sqlTempl));
	}

	@Test
	public void testUpdate() {
        String sql = " update TABLE1 t1, TABLE2 t2 set t1.XYZ=123, t2.ABC='text' where t1.id=t2.id and t1.name='toto' ";
        String sqlTempl = TemplateSqlUtil.templatizeSqlText(sql);
        assertFalse(sql.equals(sqlTempl));
    }
	
}
