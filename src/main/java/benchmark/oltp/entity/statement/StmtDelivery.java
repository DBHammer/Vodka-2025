package benchmark.oltp.entity.statement;/*
                                        * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
                                        */

import config.CommonConfig;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.mysql.jdbc.Statement;

@Setter
@Getter
public class StmtDelivery extends StmtBasic {
        public PreparedStatement stmtDeliveryBGSelectOldestNewOrder;
        public PreparedStatement stmtDeliveryBGDeleteOldestNewOrder;
        public PreparedStatement stmtDeliveryBGSelectOrder;
        public PreparedStatement stmtDeliveryBGUpdateOrder;
        public PreparedStatement stmtDeliveryBGSelectSumOLAmount;
        public PreparedStatement stmtDeliveryBGSelectOrderLine;
        public PreparedStatement stmtDeliveryBGUpdateOrderLine;
        public PreparedStatement stmtDeliveryBGUpdateCustomer;
        public PreparedStatement stmtDeliveryBGStoredProc;
        public PreparedStatement stmtDeliveryBGUpdateOrderLineForVerify;
        public PreparedStatement stmtDeliveryTest;

        public StmtDelivery(Connection dbConn, int dbType) throws SQLException {

                stmtDeliveryBGSelectOldestNewOrder = dbConn.prepareStatement(
                                "SELECT no_o_id " +
                                                "    FROM vodka_new_order " +
                                                "    WHERE no_w_id = ? AND no_d_id = ? " +
                                                "    ORDER BY no_o_id ASC");
                // Desc
                stmtDeliveryBGDeleteOldestNewOrder = dbConn.prepareStatement(
                                "DELETE FROM vodka_new_order " +
                                                "    WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?");
                stmtDeliveryBGSelectOrder = dbConn.prepareStatement(
                                "SELECT o_c_id, o_entry_d " +
                                                "    FROM vodka_oorder " +
                                                "    WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
                stmtDeliveryBGUpdateOrder = dbConn.prepareStatement(
                                "UPDATE vodka_oorder " +
                                                "    SET o_carrier_id = ? " +
                                                "    WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
                stmtDeliveryBGSelectSumOLAmount = dbConn.prepareStatement(
                                "SELECT sum(ol_amount) AS sum_ol_amount " +
                                                "    FROM vodka_order_line " +
                                                "    WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
                stmtDeliveryBGSelectOrderLine = dbConn.prepareStatement(
                                "SELECT ol_delivery_d from vodka_order_line " +
                                                "    WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
                stmtDeliveryBGUpdateOrderLine = dbConn.prepareStatement(
                                "UPDATE vodka_order_line " +
                                                "    SET ol_delivery_d = ? , ol_shipmode = ? , ol_shipinstruct = ?, access_version = 2, current_ts = ? "
                                                +
                                                "    WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
                // stmtDeliveryBGUpdateOrderLine = dbConn.prepareStatement(
                //                 "UPDATE vodka_order_line " +
                //                                 "    SET ol_delivery_d = ? , ol_shipmode = ? , ol_shipinstruct = ?, access_version = access_version + 1, current_ts = ? "
                //                                 +
                //                                 "    WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
                stmtDeliveryBGUpdateOrderLineForVerify = dbConn.prepareStatement("UPDATE vodka_order_line " +
                                "SET ol_delivery_d = ?, ol_shipmode = ?, ol_shipinstruct = ?, " +
                                "    access_version = access_version + 1, current_ts = ? " +
                                "WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? " +
                                "RETURNING access_version, (xmin)::text::bigint AS row_xmin",
                                Statement.RETURN_GENERATED_KEYS);
                stmtDeliveryBGUpdateCustomer = dbConn.prepareStatement(
                                "UPDATE vodka_customer " +
                                                "    SET c_balance = c_balance + ?, " +
                                                "        c_delivery_cnt = c_delivery_cnt + 1 " +
                                                "    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
                stmtDeliveryTest = dbConn.prepareStatement(
                                "SELECT sum(access_version), count(access_version) from vodka_order_line" +
                                                "    WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
        }
}