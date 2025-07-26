use database airflow_pipeline;
create or replace schema silver;

drop table if exists silver_time_dim ;
create table silver_time_dim(
    id integer autoincrement(1,1) primary key,
    name string not null
);

drop table if exists silver_orderstatus_dim ;
create table silver_orderstatus_dim(
    id integer autoincrement(1,1) primary key,
    name string not null
);

drop table if exists silver_orders ;
CREATE TABLE silver_orders (
    O_ORDERKEY       integer       primary key,  
    O_CUSTKEY        integer       not null,  
    O_ORDERSTATUS    int not null,  
    O_ORDERDATE      int not null,  
    O_ORDERPRIORITY  string  not null,  
    O_CLERK          string  not nulL, 
    O_SHIPPRIORITY   int       not null,  
    O_COMMENT        string,            
    O_TOTALPRICE     int       not null,

    FOREIGN KEY (O_ORDERSTATUS) REFERENCES silver_orderstatus_dim(id),
    FOREIGN KEY (O_ORDERDATE)   REFERENCES silver_time_dim(id)
);

insert into silver_time_dim(name)
select 
O_ORDERDATE
from snowflake_sample_data.tpch_sf1.orders
group by O_ORDERDATE;

insert into silver_orderstatus_dim(name)
select 
O_ORDERSTATUS
from snowflake_sample_data.tpch_sf1.orders
group by O_ORDERSTATUS;

insert into silver_orders(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT, O_TOTALPRICE)
select 
rd.O_ORDERKEY,
rd.O_CUSTKEY,
st.id,
tm.id,
rd.O_ORDERPRIORITY,
rd.O_CLERK,
rd.O_SHIPPRIORITY,
rd.O_COMMENT,
rd.O_TOTALPRICE
from snowflake_sample_data.tpch_sf1.orders rd 
inner join airflow_pipeline.silver.silver_time_dim tm on rd.O_ORDERDATE = tm.name
inner join airflow_pipeline.silver.silver_orderstatus_dim st on rd.O_ORDERSTATUS = st.name

