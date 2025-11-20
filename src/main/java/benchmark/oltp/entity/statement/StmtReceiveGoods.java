package benchmark.oltp.entity.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StmtReceiveGoods extends StmtBasic {
    public PreparedStatement stmtReveiveSelectOldeliveryd;
    public PreparedStatement stmtReveiveUpdateReceipdate;
    public PreparedStatement stmtReveiveUpdateComment;

    public StmtReceiveGoods(Connection dbConn, int dbType) throws SQLException {
        // PreparedStatements for ReceiveGoods
        stmtReveiveSelectOldeliveryd = dbConn.prepareStatement(
                "SELECT ol_delivery_d, ol_receipdate " +
                        "FROM vodka_order_line " +
                        "WHERE ol_w_id= ? AND ol_d_id=? AND ol_o_id=? ");
        stmtReveiveUpdateReceipdate = dbConn.prepareStatement(
                "UPDATE vodka_order_line " +
                        "SET ol_receipdate = ?, ol_returnflag = ?, access_version = 3, current_ts = ? " +
                        "WHERE ol_w_id= ? AND ol_d_id=? AND ol_o_id=? ");
        // stmtReveiveUpdateReceipdate = dbConn.prepareStatement(
        //         "UPDATE vodka_order_line " +
        //                 "SET ol_receipdate = ?, ol_returnflag = ?, access_version = 3, current_ts = ? " +
                        // "WHERE ol_w_id= ? AND ol_d_id=? AND ol_o_id=? ");
        stmtReveiveUpdateComment = dbConn.prepareStatement(
                "UPDATE vodka_oorder " +
                        "SET o_comment = ? " +
                        "WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
        
    }

    public PreparedStatement getStmtReveiveSelectOldeliveryd() {
        return stmtReveiveSelectOldeliveryd;
    }

    public PreparedStatement getStmtReveiveUpdateReceipdate() {
        return stmtReveiveUpdateReceipdate;
    }

    public PreparedStatement getStmtReveiveUpdateComment() {
        return stmtReveiveUpdateComment;
    }

    
}
