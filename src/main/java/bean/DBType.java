package bean;


import lombok.Getter;

@Getter
public enum DBType {
    DB_POSTGRES("postgres"), DB_ORACLE("oracle"), DB_MYSQL("mysql"), DB_OCEANBASE("oceanbase"), DB_TIDB("tidb"), DB_POLARDB("polardb"), DB_SQLITE("sqlite"), DB_H2("h2"), DB_DERBY("derby"), DB_HSQL("hsql"), DB_MARIADB("mariadb"), DB_CLICKHOUSE("clickhouse"), DB_SQLSERVER("sqlserver"), DB_UNKNOWN("unknown");

    private final String name;

    DBType(String name) {
        this.name = name;
    }

}
