CREATE TABLE [dbo].[Orders] (

	[OrderID] int NULL, 
	[CustomerID] int NULL, 
	[CreditCard] varchar(20) NULL, 
	[SaleAmount] decimal(10,2) NULL, 
	[OrderDate] date NULL
);