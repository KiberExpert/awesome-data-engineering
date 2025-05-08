--1-masala
select orderid,orderdate,custid,empid from Sales.Orders where YEAR(orderdate)=2015 and MONTH(orderdate)=6
--2-misol
select orderid,orderdate,custid,empid from Sales.Orders where orderdate=EOMONTH(orderdate)
--3-misol
select empid,firstname,lastname from HR.Employees where (len(lastname)-len((replace(lastname,'e',''))))>=2
--4-misol
select orderid,(qty*unitprice) as totalvalue from Sales.OrderDetails where (qty*unitprice)>=10000 order by totalvalue desc
--5-misol
select empid,lastname from HR.Employees where ASCII(left(lastname,1)) between 97 and 122
--6-misol
-- Query 1
SELECT empid, COUNT(*) AS numorders
FROM Sales.Orders
WHERE orderdate < '20160501'
GROUP BY empid;

-- Query 2
SELECT empid, COUNT(*) AS numorders
FROM Sales.Orders
GROUP BY empid
HAVING MAX(orderdate) < '20160501';
-- 1- so'rov buyurtma sanasi 20160501dan kichiklarini chiqaradi, 2-so'rov esa eng katta buyurtma sanasi 20160501 dan kichiklarini chiqaradi

--7-misol
select top 3 shipcountry,avg(freight) as avgfreight from Sales.Orders where year(orderdate)=2015 group by shipcountry order by avgfreight desc 
--8-misol
select s.orderid,s.orderdate,s.custid,(ROW_NUMBER() over(partition by custid order by s.custid)) as rownum from Sales.Orders s where s.orderdate in (select s1.orderdate from Sales.Orders s1 group by	s1.orderdate) order by s.custid
--9-misol
select empid,firstname,lastname,titleofcourtesy, (case when titleofcourtesy='Mrs.' or titleofcourtesy='Ms.' then 'Female' when titleofcourtesy='Mr.' or titleofcourtesy='Dr.' then 'Male' end) as gender from HR.Employees
--10-misol
select custid,region from Sales.Customers order by case when region IS NULL then 1 else 0 end
-- 11-misol
/*;with last_day as (
	select (ROW_NUMBER() over(PARTITION by year(orderdate) order by orderdate desc)) as ln,* from Sales.Orders 
),
ids as (
	select orderdate from last_day where ln=1
)
select * from Sales.Orders s left join ids i on i.orderdate=s.orderdate*/
select orderid,orderdate,custid,empid from Sales.Orders where orderdate=(select max(orderdate) from Sales.Orders)
--12-misol chala
select top 1 * from Sales.Orders
go 
with order_list as (
	select top 1 COUNT(orderid) as soni from Sales.Orders group by custid order by soni desc
),
order_list2 as (
	select s.custid, COUNT(s.orderid) as d from Sales.Orders s group by s.custid
)
select s.custid,s.orderid,s.orderdate,s.empid from Sales.Orders s right join order_list2 l on s.custid=l.custid right join order_list r on r.soni = l.d
--13-misol
go
with date_sale as (
	select empid from Sales.Orders where orderdate>'2016-05-01' group by empid
)
select distinct h.empid,h.firstname,h.lastname from HR.Employees h left join date_sale d on h.empid=d.empid where d.empid is null
--14-misol
go 
with e as (
	select country from HR.Employees group by country
),
c as(
	select country from Sales.Customers group by country
)
select c.country from c left join e on e.country = c.country where e.country is null
--15-misol
go 
with last_order as (
	select MAX(orderdate) as date,custid from Sales.Orders group by custid
),
last_cust as(
	select orderid from Sales.Orders o,last_order lo where o.custid=lo.custid and o.orderdate=lo.date
) 
select o.custid,o.orderid,o.orderdate,o.empid from Sales.Orders o, last_cust l where o.orderid=l.orderid order by o.custid 
--16-misol chala
select top 1 * from Sales.Customers
select top 1 * from Sales.Orders
select * from Sales.Orders where custid=13
go 
with list1 as (
	select distinct custid from Sales.Orders where YEAR(orderdate)='2016'
),
list2 as (
	select distinct custid from Sales.Orders where YEAR(orderdate)='2015' 
)
select distinct s.custid,s.companyname from Sales.Customers s right join list2 l2 on s.custid=l2.custid left join list1 l1 on s.custid=l1.custid where l1.custid is null
--17-misol
select top 1 * from Sales.Customers
select top 1 * from Sales.Orders
select top 1 * from Sales.OrderDetails
go 
with product_list as (
	select distinct orderid from Sales.OrderDetails where productid=12
),
order_list as (
	--select distinct custid from Sales.Orders s left join product_list l on s.orderid=l.orderid where l.orderid is null
	select distinct s.custid from Sales.Orders s, product_list l where s.orderid=l.orderid
)
select distinct s.custid,s.companyname from Sales.Customers s left join order_list l on s.custid=l.custid where l.custid is not null
--18-misol
select custid, ordermonth, qty, SUM(qty) over(partition by custid order by ordermonth) as runqty from Sales.CustOrders order by custid, ordermonth
--19-misol
/* 
IN-da to'plam tartiblanadi
EXISTS-da esa to'plam saralanmaydi
*/
--20-misol
select custid,orderdate,orderid,ABS(datediff(day,LAG(orderdate) OVER (PARTITION BY custid ORDER BY custid),orderdate)) as diff from Sales.Orders
--21-misol
CREATE TABLE [dbo].[Employee](
	[ID] [int] NULL,
	[Name] [nvarchar](50) NULL,
	[Salary] [int] NULL
) ON [PRIMARY]
GO
CREATE TABLE [dbo].[Projects](
	[Project_ID] [int] NULL,
	[Employee_ID] [int] NULL
) ON [PRIMARY]
GO

with project_list as (
	select Employee_ID,COUNT(distinct Project_ID) as soni from dbo.Projects group by Employee_ID
),
project_list2 as (
	select * from project_list where soni>=3
)
select top 3 emp.ID,emp.Name,emp.Salary from dbo.Employee emp, project_list2 l where emp.ID=l.Employee_ID order by emp.Salary
--22-misol
SELECT * from  tblPerson

with male as(
	select distinct  Parent from tblPerson where ChildGender = 'Male'
),
female as(
	select distinct Parent from tblPerson where ChildGender = 'Female'
)
select f.Parent from male m inner join female f on m.Parent = f.Parent