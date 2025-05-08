--1-misol
select ProductID,count(StandardCost) as TotalPriceChanges from dbo.ProductCostHistory group by ProductID
--2-misol
select CustomerID,COUNT(OrderID) as TotalOrders from dbo.Orders group by CustomerID
--3-misol
go 
with list as (
	select OrderID,min(OrderDate) as firstvalue,MAX(OrderDate) as lastvalue from Orders group by OrderID
),
list_date as (
	select od.ProductID,MIN(l.firstvalue) as firtsvalue,MAX(l.lastvalue) as lastvalue from OrderDetails od right join   list l on od.OrderID=l.OrderID group by od.ProductID
)
--select * from list_date
select ps.ProductID,ld.firtsvalue,ld.lastvalue from Products ps right join list_date ld on ps.ProductID=ld.ProductID order by ps.ProductID
--4-misol
go 
with list as (
	select OrderID,min(OrderDate) as firstvalue,MAX(OrderDate) as lastvalue from Orders group by OrderID
),
list_date as (
	select od.ProductID,MIN(l.firstvalue) as firtsvalue,MAX(l.lastvalue) as lastvalue from OrderDetails od right join   list l on od.OrderID=l.OrderID group by od.ProductID
)
select ps.ProductID,ps.ProductName,ld.firtsvalue,ld.lastvalue from Products ps right join list_date ld on ps.ProductID=ld.ProductID order by ps.ProductID
--5-misol
select ProductID,StandardCost from dbo.ProductCostHistory where StartDate<='2012-04-15' and EndDate>='2012-04-15'
--6-misol
select ProductID,StandardCost from dbo.ProductCostHistory where (StartDate<='2014-04-15' and EndDate>='2014-04-15') or (StartDate<='2014-04-15' and EndDate is null)
--7-misol
select CONCAT(YEAR(StartDate),'/',MONTH(StartDate)) as ProductListPriceMonth, COUNT(ModifiedDate) as totalrows from ProductListPriceHistory group by YEAR(StartDate), MONTH(StartDate)
--8-misol
select top 1 * from ProductListPriceHistory
select  top 1 * from Calendar
go 
with list as (
	select COUNT(distinct ProductID) as soni,YEAR(StartDate) as year,MONTH(StartDate) as month from ProductListPriceHistory group by YEAR(StartDate),MONTH(StartDate)
),
list2 as (
	select distinct c.CalendarMonth,l.soni from Calendar c right join list l on year(c.CalendarDate)=l.year and month(c.CalendarDate)=l.month
),
minn as (
	select MIN(CalendarMonth) as min from list2 
),
maxx as (
	select max(CalendarMonth) as max from list2 
)
select distinct c.CalendarMonth,l2.soni from minn, maxx,Calendar  c left join list2 l2 on c.CalendarMonth=l2.CalendarMonth 
where c.CalendarMonth>=minn.min and c.CalendarMonth<=maxx.max order by c.CalendarMonth

--9-misol
select ProductID,ListPrice from ProductListPriceHistory where EndDate is null
--10-misol
select pt.ProductID,pt.ProductName from ProductListPriceHistory ph, Product pt where ph.ProductID!=pt.ProductID
--11-misol
go 
with list as(
	Select
	ProductID
	,StandardCost
	From ProductCostHistory
	Where
	'2014-04-15' Between StartDate and IsNull(EndDate, getdate())
)
select ph.ProductID from ProductCostHistory ph left join list l on ph.ProductID=l.ProductID where l.ProductID is null group by ph.ProductID
--12-misol
go 
with list as (
	select ProductID,COUNT(ProductID) as soni from ProductListPriceHistory where EndDate is null group by ProductID
)
select ProductID from list where soni>1
--13-misol
select top 1 * from SalesOrderDetail
select top 1 * from ProductSubCategory
select top 1 * from Product
select top 1 * from SalesOrderHeader
go 
--
SELECT p.ProductID,
       p.ProductName,
       MIN(soh.OrderDate) AS minDate,
       MAX(soh.OrderDate) AS maxDate
FROM SalesOrderDetail sod
LEFT JOIN SalesOrderHeader soh ON sod.SalesOrderId = soh.SalesOrderId
RIGHT JOIN Product p ON p.ProductID = sod.ProductID
GROUP BY p.ProductID,
         p.ProductName
ORDER BY 2;
--14-misol
	select p.ProductID,p.ProductName,p.ListPrice as Prod_ListPrice,ph.ListPrice as PriceHist_LatestListPrice, (p.ListPrice-ph.ListPrice) as Diff from Product p right join ProductListPriceHistory ph on p.ProductID=ph.ProductID where ph.EndDate is null and (p.ListPrice-ph.ListPrice)>0
--15-misol chala
select top 1 * from SalesOrderHeader
select top 1 * from SalesOrderDetail
select top 1 * from Product
select * from ProductListPriceHistory where ProductID=726
go
with list as (
	select p.ProductID,p.ProductName,CONVERT(date, soh.OrderDate) as OrderDate,MIN(plh.StartDate) as SellStartDate,MAX(plh.EndDate) as SellEndDate from Product p right join SalesOrderDetail sod on p.ProductID=sod.ProductID left join SalesOrderHeader soh on sod.SalesOrderID=soh.SalesOrderID left join ProductListPriceHistory plh on p.ProductID=plh.ProductID 
group by p.ProductID,p.ProductName,soh.OrderDate
)
select ProductID,ProductName,OrderDate,SellStartDate,SellEndDate,ABS(IIF(OrderDate<SellStartDate,datediff(day,orderdate,SellStartDate),datediff(day,orderdate,SellEndDate))) as qty from list where OrderDate<SellStartDate or OrderDate>SellEndDate
--16-misol
select top 1 * from Sales.SalesOrderDetail
select top 1 * from Sales.SalesOrderHeader
select top 1 * from Production.Product
go 
with list as (
	select SalesOrderID,ProductID from Sales.SalesOrderDetail 
),
list2 as (
	select sh.OrderDate,l.ProductID from Sales.SalesOrderHeader sh right join list l on sh.SalesOrderID=l.SalesOrderID
),
list3 as(
	select pp.ProductID,l.OrderDate,pp.SellStartDate,pp.SellEndDate from Production.Product pp left join list2 l on pp.ProductID=l.ProductID where l.OrderDate < pp.SellStartDate or l.OrderDate >pp.SellEndDate
)
select ProductID,OrderDate,SellStartDate,SellEndDate,iif(OrderDate<SellStartDate,DATEDIFF(DAY,OrderDate,sellstartdate),datediff(day,sellenddate,orderdate)) as qty,iif(OrderDate<SellStartDate,'Sold before start date','Sold after end date') as ProblemType from list3 order by ProductID
--17-misol chala
select  * from Sales.SalesOrderHeader
go 
with list as (
	select count(SalesOrderID) as TotalOrderWithTime from SalesOrderHeader where CONVERT(char(10), OrderDate, 108)!='00:00:00'
),
list2 as (
	select COUNT(SalesOrderID) as TotalOrders from SalesOrderHeader
)
select l.TotalOrderWithTime,l2.TotalOrders,cast(cast(l.TotalOrderWithTime as float)/cast(l2.TotalOrders as float) as float) as PercentOrdersWithTime from list l, list2 l2
--18-misol
Select
Product.ProductID
,ProductName
,ProductSubCategoryName
,FirstOrder = Convert(date, Min(OrderDate))
,LastOrder = Convert(date, Max(OrderDate))
From Product
left Join SalesOrderDetail Detail
on Product.ProductID = Detail.ProductID
left Join SalesOrderHeader Header
on Header.SalesOrderID = Detail .SalesOrderID
left Join ProductSubCategory
on ProductSubCategory .ProductSubcategoryID = Product.ProductSubCategoryID
Where
Color = 'Silver'
Group by
Product.ProductID
,ProductName
,ProductSubCategoryName
Order by LastOrder desc
--19-misol
select ProductID,Name as ProductName,StandardCost,ListPrice,(ListPrice-StandardCost) as RawMargin,((ListPrice-StandardCost)/ListPrice*100) as clon,NTILE(4) over(partition by ((ListPrice-StandardCost)/ListPrice*100) order by ((ListPrice-StandardCost)/ListPrice)) Quartile from Production.Product where (ListPrice-StandardCost)>0 order by Name
--20-misol
go 
with list as(
	select CustomerID,COUNT(CustomerID) as soni from SalesOrderHeader group by CustomerID having COUNT(distinct SalesPersonEmployeeID)>1
)
select c.CustomerID,CONCAT(c.FirstName,' ',c.LastName) as CustomerName,l.soni as TotalDifferentSalesPeople from Customer c right join list l on c.CustomerID=l.CustomerID order by CustomerName,TotalDifferentSalesPeople
--21-misol
Select top 100
Customer.CustomerID
,CustomerName = FirstName + ' ' + LastName
,OrderDate
,SalesOrderHeader.SalesOrderID
,SalesOrderDetail.ProductID
,Product.ProductName
,LineTotal
From SalesOrderHeader
Join SalesOrderDetail
on SalesOrderHeader .SalesOrderID = SalesOrderDetail .SalesOrderID
Join Customer
on Customer.CustomerID = SalesOrderHeader.CustomerID
Join Product
on Product.ProductID = SalesOrderDetail.ProductID
Order by
SalesOrderHeader.CustomerID
,OrderDate
--22-misol
select distinct ProductName from Product group by ProductName having COUNT(ProductID)=2
--23-misol
go 
with list as (
	select distinct ProductName from Product group by ProductName having COUNT(ProductID)>1	
)
select p.ProductName,MAX(p.ProductID) from Product p right join list l on p.ProductName=l.ProductName group by p.ProductName
--24-misol
go 
with list as (
	select ProductID , COUNT(distinct ModifiedDate) as TotalPriceChanges from ProductCostHistory group by ProductID 
)
select TotalPriceChanges,COUNT(distinct ProductID) as TotalProducts from list group by TotalPriceChanges order by TotalPriceChanges
--25-misol
select ProductID,ProductNumber,iif(PATINDEX('%-%',ProductNumber)=0,0,8) as HyphenLocation,IIF(CHARINDEX('-',ProductNumber)=0,ProductNumber,left(ProductNumber,(charindex('-',ProductNumber)-1))) as BaseProductNumber,IIF(PATINDEX('%-%',ProductNumber)!=0,right(ProductNumber,len(ProductNumber)-PATINDEX('%-%',ProductNumber)),null) as Size from Product where ProductID>533
--26-misol
go 
with list as (
	select distinct ProductSubcategoryID from ProductSubcategory where ProductCategoryID=3
),
list2 as (
	select distinct IIF(CHARINDEX('-',ProductNumber)=0,ProductNumber,left(ProductNumber,(charindex('-',ProductNumber)-1))) as BaseProductNumber from Product p right join list l on p.ProductSubcategoryID=l.ProductSubcategoryID 
)
select distinct l.BaseProductNumber, COUNT(p.ProductID) as TotalSizes from Product p, list2 l where p.ProductNumber like l.BaseProductNumber+'%' group by l.BaseProductNumber
--27-misol
go 
with list as(
	select ProductID,StartDate,StandardCost,LAG(StandardCost) OVER(partition by ProductID ORDER BY ProductID) PreviousStandardCost from ProductCostHistory

)
select p.ProductID,p.ProductName,COUNT(l.StandardCost) as TotalCostChanges from Product p right join list l on p.ProductID=l.ProductID group by p.ProductID,p.ProductName order by p.ProductID
--28-misol
go 
with list as (
	select (ROW_NUMBER() over(partition by productid order by startdate)) as soni,ProductID,StartDate,StandardCost,(LAG(StandardCost) over(partition by productid order by startdate)) as PreviousStandardCost,ABS((StandardCost-(LAG(StandardCost) over(partition by productid order by startdate)))) as PriceDifference from ProductCostHistory
)
select ProductID,StartDate as CostChangeDate,StandardCost,PreviousStandardCost,PriceDifference from list where soni=2 order by ProductID asc
--29-misol
;with FraudSuspects as (
Select CustomerID
From Customer
Where
CustomerID in (
29401
,11194
,16490
,22698
,26583
,12166
,16036
,25110
,18172
,11997
,26731
)
),
SampleCustomers as (
	select top 100 c.CustomerID from Customer c, FraudSuspects f where c.CustomerID!=f.CustomerID group by c.CustomerID
),
list as (
	select * from SampleCustomers union all select * from FraudSuspects
)
select c.CustomerID,c.FirstName,c.LastName,c.AccountNumber,c.ModifiedDate from Customer c right join list l on c.CustomerID=l.CustomerID
--30-misol
go 
with list as (
	select p.ProductID,c.CalendarDate from ProductListPriceHistory p left join Calendar c on p.StartDate<=c.CalendarDate  and p.EndDate>=c.CalendarDate
)
select ProductID,CalendarDate,COUNT(ProductID) as TotalRows from list where CalendarDate is not null group by ProductID,CalendarDate having COUNT(ProductID)>1
--31-misol
go 
with list as (
	--select OrderID,OrderDate,CONCAT(CONCAT(YEAR(CONVERT(date,OrderDate)),'/',MONTH(convert(date,Orderdate))),' - ',DATENAME(month,convert(date,Orderdate))) cln from Orders
	select SalesOrderID,OrderDate,CONCAT(CONCAT(YEAR(CONVERT(date,OrderDate)),'/',MONTH(convert(date,OrderDate))),' - ',DATENAME(month,convert(date,OrderDate))) cln from Sales.SalesOrderHeader
)
select cln,COUNT(distinct SalesOrderID) as TotalOrders,COUNT(SalesOrderID) as RunningTotal from list group by cln
--32-misol chala 

---33-misol
go
with list as (
	select SalesOrderID,TerritoryID,DueDate,ShipDate,IIF(DueDate>ShipDate,0,1) as OrderArrivedLate from Sales.SalesOrderHeader
),
list2 as (
	select TerritoryID,COUNT(OrderArrivedLate) as TotalLateOrders from list where OrderArrivedLate=1 group by TerritoryID
)
select l.TerritoryID,st.Name,st.CountryRegionCode as CountryCode,COUNT(SalesOrderID) as TotalOrders,l2.TotalLateOrders from Sales.SalesTerritory st right join list l on st.TerritoryID=l.TerritoryID left join list2 l2 on st.TerritoryID=l2.TerritoryID group by l.TerritoryID,st.Name,st.CountryRegionCode,l2.TotalLateOrders order by l.TerritoryID
--34-misol
-----------------------------------------
-- Solution #4
-----------------------------------------
;with Main as (
Select
SalesOrderID
,HasTimeComponent =
case
When OrderDate <> Convert(date, OrderDate)
then 1
else 0
end
From SalesOrderHeader
)
Select
TotalOrdersWithTime =Sum(HasTimeComponent )
,TotalOrders = Count(*)
,PercentOrdersWithTime =
Sum(HasTimeComponent ) * 1.0
/
Count(*)
From Main
--35-misol
go 
with list as (
	select soh.CustomerID,soh.OrderDate,soh.SalesOrderID,sod.ProductID,sod.LineTotal,psc.ProductSubCategoryName from SalesOrderHeader soh left join SalesOrderDetail sod on soh.SalesOrderID=sod.SalesOrderID 
	left join Product p on sod.ProductID=p.ProductID left join ProductSubcategory psc on p.ProductSubcategoryID=psc.ProductSubcategoryID	where soh.CustomerID in (19500,19792,24409,26785) 
),
list2 as (
	select (ROW_NUMBER() over(partition by CustomerID Order by orderdate desc,LineTotal desc)) id,* from list 
)
select c.CustomerID,CONCAT(c.FirstName,' ',c.LastName) as CustomerName,l.ProductSubCategoryName from list2 l,Customer c where l.id=1 and l.CustomerID=c.CustomerID
--36-misol
go 
with list as (
	select ot.SalesOrderID,ot.TrackingEventID,te.EventName,ot.EventDateTime from OrderTracking ot left join TrackingEvent te on ot.TrackingEventID=te.TrackingEventID 
	where ot.SalesOrderID in (68857,70531,70421)
)
select SalesOrderID,EventName,EventDateTime as TrackingEventDate,Lead(EventDateTime) OVER(partition by salesorderid ORDER BY SalesOrderID,EventDateTime) NextTrackingEventDate,IIF(Lead(EventDateTime) OVER(partition by salesorderid ORDER BY SalesOrderID,EventDateTime) is null,null,DATEDIFF(HOUR,EventDateTime,Lead(EventDateTime) OVER(ORDER BY SalesOrderID))) HoursInStage from list order by SalesOrderID,EventDateTime
--37-misol
go 
with list as (
	select ot.SalesOrderID,ot.TrackingEventID,te.EventName,ot.EventDateTime,soh.OnlineOrderFlag from OrderTracking ot left join TrackingEvent te on ot.TrackingEventID=te.TrackingEventID left join SalesOrderHeader soh on ot.SalesOrderID=soh.SalesOrderID
),
list2 as (
	select IIF(OnlineOrderFlag=0,'Offline','Online') OnlineOfflineStatus,EventName,EventDateTime as TrackingEventDate,Lead(EventDateTime) OVER(partition by salesorderid ORDER BY SalesOrderID,EventDateTime) NextTrackingEventDate,IIF(Lead(EventDateTime) OVER(partition by salesorderid ORDER BY SalesOrderID,EventDateTime) is null,null,DATEDIFF(HOUR,EventDateTime,Lead(EventDateTime) OVER(ORDER BY SalesOrderID))) HoursInStage from list
)
select OnlineOfflineStatus,EventName,AVG(HoursInStage) as AverageHoursSpentInStage from list2 group by OnlineOfflineStatus,EventName order by OnlineOfflineStatus,EventName

--38-misol
go 
with list as (
	select ot.SalesOrderID,ot.TrackingEventID,te.EventName,ot.EventDateTime,soh.OnlineOrderFlag from OrderTracking ot left join TrackingEvent te on ot.TrackingEventID=te.TrackingEventID left join SalesOrderHeader soh on ot.SalesOrderID=soh.SalesOrderID
),
list2 as (
	select IIF(OnlineOrderFlag=0,'Offline','Online') OnlineOfflineStatus,EventName,EventDateTime as TrackingEventDate,Lead(EventDateTime) OVER(partition by salesorderid ORDER BY SalesOrderID,EventDateTime) NextTrackingEventDate,IIF(Lead(EventDateTime) OVER(partition by salesorderid ORDER BY SalesOrderID,EventDateTime) is null,0,DATEDIFF(HOUR,EventDateTime,Lead(EventDateTime) OVER(ORDER BY SalesOrderID))) HoursInStage from list
),
list3 as (
	select EventName,Offline,Online  from list2 
	PIVOT (AVG(hoursInStage) for OnlineOfflineStatus in ([Offline],[Online])) as tbl 
)
--select * from list3
select EventName,AVG(Offline) as Offline,AVG(Online) as Online from list3 group by EventName
--39-misol
select  * from SalesOrderDetail where SalesOrderID=66113
go 
WITH list AS
  (SELECT sod.SalesOrderID,
          c.CustomerID,
          CONCAT(c.FirstName, ' ', c.LastName) AS CustomerName,
          psc.ProductSubCategoryName,
          sod.LineTotal
   FROM Customer c
   RIGHT JOIN SalesOrderHeader soh ON c.CustomerID=soh.CustomerID
   LEFT JOIN SalesOrderDetail sod ON soh.SalesOrderID=sod.SalesOrderID
   LEFT JOIN Product p ON sod.ProductID=p.ProductID
   LEFT JOIN ProductSubcategory psc ON p.ProductSubcategoryID=psc.ProductSubCategoryID
   WHERE c.CustomerID in (13763,
                          13836,
                          20331,
                          21113,
                          26313)
						  ),
cte as (
	select (ROW_NUMBER() over(partition by CustomerID order by CustomerID)) as tr,SalesOrderID,CustomerID,CustomerName,ProductSubCategoryName,SUM(LineTotal) as LineTotal from list group by SalesOrderID,CustomerID,CustomerName,ProductSubCategoryName
)
select t1.CustomerID, t1.CustomerName,t1.ProductSubCategoryName TopProdSubCat1,t2.ProductSubCategoryName TopProdSubCat2,t3.ProductSubCategoryName TopProdSubCat3 from cte t1 left join cte t2 on t1.CustomerID = t2.CustomerID and t2.tr = 2 left join cte t3 on t3.CustomerID = t1.CustomerID and t3.tr = 3 where t1.tr = 1
--40-misol
select top 1 * from ProductListPriceHistory
select top 1 * from Calendar
go 
with list  as (
	select (ROW_NUMBER() over(partition by productid order by productid)) as tr,* from ProductListPriceHistory
),
list2 as (
	select * from list where EndDate is null
),
list3 as (
	select l.ProductID,(select EndDate from list l3 where l3.tr=(l.tr-1) and l3.ProductID=l.ProductID) as startdate,l.StartDate as enddate from list l right join list2 l2 on l.tr=l2.tr and l.ProductID=l2.ProductID
),
list4 as (
	select * from list3 where DATEDIFF(DAY,startdate,enddate)>1
)
select l.ProductID,c.CalendarDate from Calendar c right join list4 l on c.CalendarDate>l.startdate and c.CalendarDate<l.enddate

select * from INFORMATION_SCHEMA.COLUMNS where COLUMN_NAME like '%category%'






































































































































