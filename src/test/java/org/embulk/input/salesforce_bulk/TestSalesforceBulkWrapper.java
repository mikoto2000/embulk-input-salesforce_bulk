package org.embulk.input.salesforce_bulk;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.io.InputStream;

import com.sforce.async.QueryResultList;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * TestSalesforceBulkWrapper
 */
public class TestSalesforceBulkWrapper {
    @Test
    public void testsyncQuery() throws Exception {
        // ユーザー情報をプロパティファイルから取得
        Properties p = new Properties();
        InputStream is = TestSalesforceBulkWrapper.class.
                getResourceAsStream("/user_info.properties");
        p.load(is);

        String userName = p.getProperty("username", "");
        String password = p.getProperty("password", "");

        if (userName.equals("") || password.equals("")) {
            System.exit(1);
        }

        SalesforceBulkWrapper sfbw = new SalesforceBulkWrapper(
                userName, password);
        List<Map<String, String>> queryResults = sfbw.syncQuery(
                "Account", "SELECT Id, Name FROM Account ORDER BY Id");
        sfbw.close();
        System.out.println(queryResults);
    }
}
