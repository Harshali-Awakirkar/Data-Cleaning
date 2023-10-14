CREATE DATABASE IF NOT EXISTS data_cleaning_project;
USE data_cleaning_project;

-----------------------------------------------------------------------------------------------------------------

CREATE TABLE Nashville_Housing
(
   UniqueID INT,
   ParcelID VARCHAR(100) ,
   LandUse VARCHAR(100) ,
   PropertyAddress VARCHAR(500) ,
   SaleDate VARCHAR(100) ,
   SalePrice VARCHAR(20)  ,
   LegalReference VARCHAR(100) ,
   SoldAsVacant VARCHAR(10) ,
   OwnerName VARCHAR(100) ,
   OwnerAddress VARCHAR(100),
   Acreage VARCHAR(20) ,
   TaxDistrict VARCHAR(100) ,
   LandValue VARCHAR(20) ,
   BuildingValue VARCHAR(20) ,
   TotalValue VARCHAR(20) ,
   YearBuilt VARCHAR(20) ,
   Bedrooms VARCHAR(20) ,
   FullBath VARCHAR(20) ,
   HalfBath VARCHAR(2) 
);


load data infile "C:/Excelr/Personal Projects/SQL Data Exploration/Nashville  Data Cleaning.csv"
into table nashville_housing
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\r\n'
ignore 1 rows;
   
-----------------------------------------------------------------------------------------------------------------------------------
/*
Cleaning Data in SQL Queries
*/
select * from nashville_housing;

-----------------------------------------------------------------------------------------------------------------
-- Standardize Date Format

select saledate, date(saledate)
from nashville_housing;

Update nashville_housing
set saledate = date(saledate);

--------------------------------------------------------------------------------------------------------------------------
-- Populate Property Address Data

select * 
from nashville_housing
-- where propertyaddress = ""
order by parcelid;

select propertyaddress
from nashville_housing;

update nashville_housing
set propertyaddress = nullif(propertyaddress, '');

--------------------------------------------------------------------------------------------------------------------------
-- Creating a self join to get a new column with address to add to the null property addresses

select 
	a.parcelid,
    a.propertyaddress,
    b.parcelid,
    b.propertyaddress
from nashville_housing a 
join nashville_housing b
on a.parcelid = b.parcelid
and a.uniqueid <> b.uniqueid
where a.propertyaddress is null;

-----------------------------------------------------------------------------------------------------------------
-- Updating a table with new address

update nashville_housing a
join nashville_housing b
on a.parcelid = b.parcelid
and a.uniqueid <> b.uniqueid
set a.propertyaddress = ifnull(a.propertyaddress, b.propertyaddress)
where a.propertyaddress is null;

--------------------------------------------------------------------------------------------------------------------------
-- Breaking out addresses into individual columns (Address, City, State)

select propertyaddress
from nashville_housing;

select substring_index(propertyaddress, ',', 1) as Address,
	substring_index(propertyaddress, ',', -1) as Address
from nashville_housing;

alter table nashville_housing
add PropertySplitAddress nvarchar(255);

update nashville_housing
set propertysplitaddress =  substring_index(propertyaddress, ',', 1) ;

alter table nashville_housing
add propertysplitcity nvarchar(255);

update nashville_housing
set propertysplitcity = substring_index(propertyaddress, ',', -1);

--------------------------------------------------------------------------------------------------------------------------
-- Splitting owner address

select * 
from nashville_housing;

select owneraddress,
	substring_index(owneraddress, ',', 1) as Address,
    substring_index(substring_index(owneraddress, ',', 2), ',', -1) as Address2,
    substring_index(owneraddress, ',', -1) as Address3
from nashville_housing;

alter table nashville_housing
add Ownersplitaddress nvarchar(255);

update nashville_housing
set Ownersplitaddress = substring_index(owneraddress, ',', 1);

alter table nashville_housing
add Ownersplitcity nvarchar(255);

UPDATE nashville_housing
set Ownersplitcity = substring_index(substring_index(owneraddress, ',', 2), ',', -1);

alter table nashville_housing
add ownersplitstate nvarchar(255);

update nashville_housing
set Ownersplitstate = substring_index(owneraddress, ',', -1);
select * from nashville_housing;

----------------------------------------------------------------------------------------------------------------------------
-- Changing Y and N to Yes and No in Sold as Vacant column

select distinct(soldasvacant), count(soldasvacant)
from nashville_housing
group by soldasvacant
order by 2;

select soldasvacant,
	case 
		when soldasvacant = 'Y' then 'Yes'
        when soldasvacant = 'N' then 'No'
        else soldasvacant
	end
from nashville_housing;

update nashville_housing
set soldasvacant = case 
		when soldasvacant = 'Y' then 'Yes'
        when soldasvacant = 'N' then 'No'
        else soldasvacant
	end;
    
--------------------------------------------------------------------------------------------------------------------------
-- Remove Duplicates

with cte1 as (select *, 
	row_number() over (partition by parcelid, propertyaddress, saleprice, saledate, legalreference order by uniqueid) row_num
from nashville_housing)
select * from cte1
where row_num > 1;

with cte1 as (select *, 
	row_number() over (partition by parcelid, propertyaddress, saleprice, saledate, legalreference order by uniqueid) row_num
from nashville_housing)
delete from cte1
where row_num > 1;

with cte1 as (select *, 
	row_number() over (partition by parcelid, propertyaddress, saleprice, saledate, legalreference order by uniqueid) row_num
from nashville_housing)
select * from cte1
where row_num > 1;

---------------------------------------------------------------------------------------------------------------------------------
-- Delete unused column

alter table nashville_housing
drop column Owneraddress, 
Drop column Taxdistrict,
Drop column PropertyAddress;