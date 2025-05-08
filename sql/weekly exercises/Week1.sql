Hometask 1
a) CREATE DATABASE HR;
b)
USE HR
GO
CREATE TABLE EMPLOYEE(
	Employee_ID int not null Primary key,
	Employee_FName varchar(50) not null,
	Employee_lName varchar(50) not null,
	Employee_HireDate date not null,
	Employee_Title varchar(50) 
)
CREATE TABLE SKILL(
	Skill_ID int identity(100,10) primary key,
	Skill_Name varchar(90) not null,

)
alter table SKILL add Skill_Description varchar(120) not null
CREATE TABLE CERTIFIED(
	Employee_ID int not null foreign key references EMPLOYEE(Employee_ID),
	Skill_ID int not null foreign key references SKILL(Skill_ID),
	Certified_Date date not null 
)

INSERT INTO SKILL VALUES
('basic database management','create and manage database user accounts'),('basic web design','create and maintain HTML and CSS documents'),
('advanced spreadsheets','use of advanced functions, user-defined funtions and macroing'),('basic process modeling','create core bussiness process models using standart libraries'),
('basic database design','create simple data models'),
('master database programming','create and manage database user accounts'),('basic spreatsheets','create and maintain HTML and CSS documents'),
('basic c# programming','use of advanced functions, user-defined funtions and macroing'),('advanced datbase management','create core bussiness process models using standart libraries'),
('advanced process modeling','create simple data models'),
('advanced c# programming','use of advanced functions, user-defined funtions and macroing'),('basic database manipulation','create core bussiness process models using standart libraries'),
('advanced database manipulation','create simple data models')

INSERT INTO EMPLOYEE VALUES
(03373,'Johnny','Jones','3/15/2002','DBA'),(04893,'Johnny','Jones','6/11/2004','DBA'),
(06234,'Johnny','Jones','8/10/2005','DBA'),(08273,'Johnny','Jones','7/28/2006','DBA'),(09002,'Johnny','Jones','5/20/2010','DBA'),
(09283,'Johnny','Jones','7/4/2010','DBA'),(09382,'Johnny','Jones','8/2/2010','DBA'),(10282,'Johnny','Jones','4/11/2011','DBA'),
(13383,'Johnny','Jones','3/12/2012','DBA'),(13567,'Johnny','Jones','9/30/2012','DBA'),(13932,'Johnny','Jones','9/29/2013','DBA'),
(14311,'Johnny','Jones','9/1/2014','DBA')

INSERT INTO CERTIFIED (Employee_ID,Skill_ID,Certified_Date) VALUES
(09002,110,'5/16/2013'),(09002,120,'5/16/2013'),(09382,140,'8/2/2012'),(09382,210,'8/2/2012'),(09382,220,'5/1/2013'),
(13383,170,'3/12/2014'),(13567,130,'9/30/2014'),(13567,140,'5/23/2015'),(14311,110,'9/1/2016')

d)
alter table Skill add constraint CF_Skill_ID check(Skill_ID>=100 and Skill_ID<=220)
c) 
alter table Certified add constraint CF_Employee_ID foreign key(Employee_ID) references Employee(Employee_ID)
e)
alter table Employee add constraint CF_Employee_Title default 'None title' for Employee_Title

Hometask2
a) create database Store
b)
use Store
go
create table Region(
	region_code int identity(1,1) primary key,
	region_descript varchar(50) not null
)
create table Employee(
	emp_code int identity(1,1) primary key,
	emp_title varchar(10) not null,
	emp_lname varchar(50) not null,
	emp_fname varchar(50) not null,
	emp_initial char(50),
	emp_dob date not null,
	store_code int 
)
insert into Employee values('mr','william','john','w','21-may-64','3')
create table Store (
	store_code int identity(1,1) primary key,
	store_name varchar(50) not null,
	store_ytd_sales float not null,
	ragion_code int ,
	emp_code int ,
	foreign key(emp_code) references Employee(emp_code),
	foreign key(ragion_code) references Region(region_code)
)
insert into Region(region_descript) values('region')
insert into Store(store_name,store_ytd_sales,ragion_code,emp_code) values('access jumction',1003455.76,1,1)

c) 
alter table Employee add constraint cf_store_default_code default 3 for store_code
e)
alter table Store add constraint cf_store_ytd check (store_ytd_sales>0)


