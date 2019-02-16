import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestHive {
    private Connection connection;
    private PreparedStatement ps;
    private ResultSet rs;
    //创建连接
    @Before
    public void getConnection() {
        try {

            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection("jdbc:hive2://192.168.40.3:10000/", "hadoop", "hadoop");
            System.out.println(connection);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //关闭连接
    public void close() {
        try {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 创建表
    @Test
    public void createTable() {
        String sql = "create table wk110.goods2(id int,name string) row format delimited fields terminated by '\t' ";
        try {
            ps = connection.prepareStatement(sql);
            ps.execute(sql);
            close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    // 删除表
    @Test
    public void dropTable() {
        String sql = "drop table wk110.goods2";
        try {
            ps = connection.prepareStatement(sql);
            ps.execute();
            close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //添加数据
    @Test
    public void insert() throws SQLException{
        String sql = "load data local inpath '/home/hadoop/goods.txt' into table wk110.goods2";
        //记得先在文件系统中上传goods.txt
        ps = connection.prepareStatement(sql);
        ps.execute();
        close();
    }
    //查询
    @Test
    public void find() throws SQLException {
        String sql = "select * from wk110.goods2 ";
        ps = connection.prepareStatement(sql);
        rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getObject(1) + "---" + rs.getObject(2));
        }
        close();
    }


}
