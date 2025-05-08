--1 view
select top 1 * from Sales.SalesPerson
create view vW_SalesCustomer as 
select CustomerID,TerritoryID,rowguid from Sales.Customer

create view vW_SalesPerson
with schemabinding
as 
select BusinessEntityID,SalesYTD,ModifiedDate from Sales.SalesPerson

insert into vW_SalesPerson(BusinessEntityID,SalesYTD) values(128,128)
select * from vW_SalesPerson where BusinessEntityID=128

--2 store proc
create proc sp_SalesPersonAll
as 
begin 
select * from Sales.SalesPerson
end

exec sp_SalesPersonAll

create proc sp_SalesPersontTerritoryID
@id int
as 
begin
select * from Sales.SalesPerson sp where sp.TerritoryID=@id
end

exec sp_SalesPersontTerritoryID 4

--3 Scalar UDF
create function func_SalesPerson (@territoryid int)
returns table 
as 
return 
select * from Sales.SalesPerson where TerritoryID=@territoryid

select * from func_SalesPerson(4) 
select * from func_SalesPerson(4) where BusinessEntityID=276

--4 Multi-Statement UDF
create function func_SalesPersonAll()
returns table 
as 
return
select * from Sales.SalesPerson

select * from func_SalesPersonAll()
--funksiyaga qiymat shu struktura asosida qo'shiladi, bu yerda ikkilamchi kalit borligi uchun xatolik beradi :(
insert into func_SalesPersonAll() values(2001,null,null,0.00,0.00,20016,0.00,'2C996CC1-FB21-4D04-862D-19D7EC2354E2','2023-03-27 16:17:42.777') 

--5 One DML trigger of your choice
create table scan_del_SalesPerson(
	id int identity(1,1) primary key,
	row_id nvarchar(20) not null
)

create trigger tr_SalerPerson
on Sales.SalesPerson
after delete 
as
begin
declare @id int 

select @id=(select BusinessEntityID from deleted)

insert into scan_del_SalesPerson values(@id)
end
 drop trigger if exists tr_SalerPerson


-- Query: View vs Stored Procedure vs UDF vs Trigger
--   view- bu asosan jadvaldagi qiymatlarni ko'rish uchun ishlatiladi,u asosan o'zida qiymat saqlamaydi. 
--   stored proc - protsedura 0 yoki n qiymatlarni qaytarishi mumkin,prodsedura uchun kirish vachiqish parametrlari mavjud.tanlash imkoniyati mavjud.
--   udf- prodsedurani chaqirishi mumkin, where,having,selectda foydalanish mumkin.
--  triggerlar - asosan log yozish uchun xizmat qiladi.Ma'lumotlar bazasini o'zgarishlarini qayd etib boradi.

