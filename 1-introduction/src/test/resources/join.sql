
    SELECT u.id AS user_id, u.name AS user_name, o.id as order_id
    FROM mysql.users u
             INNER JOIN csv.orders o ON u.id = o.user_id
    WHERE u.age > 50

    
    LogicalProject(user_id=[$0],user_name=[$1], order_id=[$5])
        LogicalFilter(condition=[>($4, 50)])
            LogicalJoin(condition=[=($0, $6)], jinType=[inner])
                LogicalTableScan(table=[[USERS]])
                LogicalTableScan(table=[[ORDERS])



    LogicalProject(user id=[$0],user_name=[$1],order_id=[$5])
        LogicalJoin(condition=[=($0, $6)], joinType=[inner])
            LogicalProject(ID=[$O], NAME=[$1])
                LogicalFilter(condition=[>($4, 50)])
                    LogicalTableScan(table=[[USERS]])
            LogicalProject(ID=[$O], USER_ID=[$1])
                LogicalTableScan(table=[[ORDERS]])
    
    
    
    
    SparkProject(user_id=[$0],user_name=[$l], order_id=[$2])
        SparkJoin(condition=[=($0,S3)], joinType=[inner])
            JdbcToSparkBridge
                JdbcProject(ID=[$0],NAME=[$1])
                    JdbcFilter(condition=[>($0, 50)])
                        JdbcTableScan(table=[[acme, site, PUBLIC,USERS]])
            JdbcToSparkBridge
                JdbcProject(ID=[$O], USER_ID=[$1])
                    JdbcTableScan(table=[[acme, cart, PUBLIC,ORDERS]])




