use airflow_pipeline.silver;
drop function if exists find_customer(int);
create or replace function find_customer(customer_id int)
returns string 
as 
$$ 
    select 
        C_NAME
    from snowflake_sample_data.tpch_sf1.customer
    where C_CUSTKEY=customer_id
$$;

select find_customer(60001);

drop procedure if exists customer_details();
create or replace procedure customer_details()
returns string 
as 
$$
begin

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
        left join airflow_pipeline.silver.silver_orders rs on rd.O_ORDERKEY = rs.O_ORDERKEY
    where rs.O_ORDERKEY is null;
    
return 'Customer details find successfully';
end;
$$;


call customer_details()
