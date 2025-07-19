
# Data Cleaning 
# https://www.kaggle.com/datasets/swaptr/layoffs-2022

select *
from layoffs;


# first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

create table layoffs_staging
like layoffs;

insert layoffs_staging
select * from layoffs;
select * from layoffs_staging;

#now when we are data cleaning we usually follow a few steps
# 1. check for duplicates and remove any
# 2. standardize data and fix errors
# 3. Look at null values and see what 
# 4. remove any columns and rows that are not necessary - few ways



# 1. Remove Duplicates

# First let's check for duplicates

with duplicate_cte as 
(
select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

with duplicate_cte as 
(
select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

# these are the ones we want to delete where the row number is > 1 or 2or greater essentially
# now i created a 3rd table to delete the duplicate data:


CREATE TABLE `layoffs_` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_
select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_
where row_num > 1;


#  now that we have this we can delete rows were row_num is greater than 2


delete
from layoffs_
where row_num > 1;

# 2. Standardize Data

select distinct(trim(company))
from layoffs_;
select company, (trim(company))
from layoffs_;

update layoffs_
set company = (trim(company));
select company
from layoffs_;

select distinct(industry)
from layoffs_;

# I noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto

select *
from layoffs_
where industry like 'crypto%';

update layoffs_
set industry = 'crypto'
where industry like 'crypto%';

select distinct(country)
from layoffs_
order by 1;

# everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
select distinct(country),trim(trailing '.' from country)
from layoffs_
order by 1;


update layoffs_
set country = trim(trailing '.' from country)
where  country like 'united states%';

# Let's also fix the date columns:
# we can use str to date to update this field
select `date`, 
str_to_date(`date`, '%m,%d,%Y')
from layoffs_;

select `date`, 
str_to_date(`date`, '%m/%d/%Y')
from layoffs_;


# now we can convert the data type properly


update layoffs_
set `date`= str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_
modify column `date` date;



#3. Look at Null Values

# now we need to populate those nulls if possible

select *
from layoffs_
where total_laid_off  is null
and percentage_laid_off is null;

select *
from layoffs_
where industry is null
or industry = '';

select *
from layoffs_
where company = 'airbnb';

update layoffs_
set industry = 'travel' 
where company = 'airbnb';

select *
from layoffs_
where company = 'juul';

update layoffs_
set industry = 'consumer' 
where company = 'juul';
select *
from layoffs_
where company = 'carvana';
update layoffs_
set industry = 'transportation' 
where company = 'carvana';

select *
from layoffs_
where company = 'Bally''s Interactive';

select *
from layoffs_
where total_laid_off  is null
and percentage_laid_off is null;



delete
from layoffs_
where total_laid_off  is null
and percentage_laid_off is null;
select *
from layoffs_;


# removed any extra or unwanted column

alter table layoffs_
drop column row_num;

SELECT * 
FROM world_layoffs.layoffs_;