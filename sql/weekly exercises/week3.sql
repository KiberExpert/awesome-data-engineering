--1.1-misol
select empid,firstname,lastname,n.n as n from HR.Employees h cross join dbo.Nums n where n.n<=5
--1.2-misol
select top 1 * from HR.Employees h
select top 1 * from dbo.Nums
select empid,firstname,lastname,DATEadd(day,n.n,'2016-06-11') as dt from HR.Employees h cross join dbo.Nums n where n.n<=5 order by h.empid
--2-misol
SELECT C.custid, C.companyname, O.orderid, O.orderdate
FROM Sales.Customers AS C
  INNER JOIN Sales.Orders AS O
    ON C.custid = O.custid;
--3-misol
go 
with cus_list as (
	select distinct custid from Sales.Customers where country='USA'
),
order_list as (
	select s.orderid from Sales.Orders s right join cus_list c on s.custid=c.custid where s.custid=c.custid
),
list as (
	select s.orderid,s.custid from Sales.Orders s, order_list o where s.orderid=o.orderid
),
qty as (
	select l.orderid,l.custid,s.qty from Sales.OrderDetails s right join list l on s.orderid=l.orderid
)
select custid,COUNT(orderid) as numorders,SUM(qty) as totalqty from qty group by qty.custid
--4-misol
select top 1 * from Sales.Customers
select top 1 * from Sales.Orders
select sc.custid,sc.companyname,so.orderid,so.orderdate from Sales.Customers sc left join Sales.Orders so on sc.custid=so.custid
--5-misol
select sc.custid,sc.companyname,so.orderid,so.orderdate from Sales.Customers sc left join Sales.Orders so on sc.custid=so.custid where so.orderid is null
--6-misol
select sc.custid,sc.companyname,so.orderid,so.orderdate from Sales.Customers sc right join Sales.Orders so on sc.custid=so.custid where so.orderdate='2016-02-12'
--7-misol
go 
with order_list as (
	select orderid,orderdate,custid from Sales.Orders where orderdate='2016-02-12'
)
select sc.custid,sc.companyname,l.orderid,l.orderdate from Sales.Customers sc left join order_list l on sc.custid=l.custid
--8-misol
SELECT C.custid, C.companyname, O.orderid, O.orderdate
FROM Sales.Customers AS C
  LEFT OUTER JOIN Sales.Orders AS O
    ON O.custid = C.custid
WHERE O.orderdate = '20160212'
   OR O.orderid IS NULL;
--bu yerda 2016-02-12 sanassidagi savdoni va savdo qilmagan xaridorlarni chiqaradi. masala shartida esa qolgan xaridorlar ham chqarilsin deyilgan.

--9-misol
go 
with order_list as (
	select orderid,orderdate,custid from Sales.Orders where orderdate='2016-02-12'
)
select sc.custid,sc.companyname,IIF(l.orderdate is null,'No','Yes') as HasOrderOn20160212 from Sales.Customers sc left join order_list l on sc.custid=l.custid order by custid
--10-misol
SELECT 'foo' AS bar UNION SELECT 'foo' AS bar
SELECT 'foo' AS bar UNION all SELECT 'foo' AS bar
/*
union - operatori distinctdan foydalanadi 
union all - bu esa dublikat tarzida kelgan ma'lumotlarni saralamaydi. ya'ni dublikat qilmaydi.
*/
--11-misol
with r as(select MName from dbo.movie where Roles= 'Actor' group by mname having COUNT(distinct AName) =2 )
select m.* from dbo.Movie m right join r on r.MName = m.MName
--12-misol
--Create table
select * from testmax
go 
with list as (
	select year1,col,ball from testmax unpivot(ball for col in ([Max1],[Max2],[Max3])) as unpvt
)
select year1,MAX(ball) as MaxValue from list group by year1
--13-misol
select * from dbo.person ps where ps.mgrid in (select empid from dbo.person where empsalary<ps.empsalary)
