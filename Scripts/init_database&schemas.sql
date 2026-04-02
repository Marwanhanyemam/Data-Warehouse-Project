/*
Script purpose: This script creates Database named "Datawarehouse". In addition to, 




*/
use master;
go
create database Datawarehouse;
go

use Datawarehouse;
go

create schema bronze;
go

create schema silver;
go

create schema gold;
go