package benchmark.oltp.entity.statement;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StmtVodkaTime extends StmtBasic {
    public PreparedStatement stmtUpdateVodkaTime;

    public StmtVodkaTime(Connection dbConn, int dbType) throws SQLException {

        stmtUpdateVodkaTime = dbConn.prepareStatement(
                "UPDATE vodka_time " +
                "    SET new_order = new_order + ?, payment = payment + ?");
        // stmtUpdateVodkaTime = dbConn.prepareStatement(
        //         "UPDATE vodka_time " +
        //         "    SET new_order = ?, payment = ?");
    }

    public PreparedStatement getStmtUpdateVodkaTime() {
        return stmtUpdateVodkaTime;
    }
}

