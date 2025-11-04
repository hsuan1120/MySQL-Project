-- Data Cleaning
SELECT  * 
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove Any Columns

-- Since the original table should remain untouched during cleaning,
-- a seperate staging table withe the same schema is created.
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT*
FROM layoffs_staging;

-- Insert the orginal data into staging table
INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- step1 : Remove Duplicates
-- partition by: grouping key; row_number:assign a sequential number within each group 
-- if row_number>2, the record is considered a duplicate
select *,
row_number() over(
partition by industry, total_laid_off, percentage_laid_off, `date`) as row_num 
from layoffs_staging;

-- The CTEs may need refinement because the current partitioning is not grangular
-- enough and therefore produces duplicates.
with duplicate_cte as
(
select *,
row_number() over(
partition by industry, location, 
total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) as row_num 
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;

-- Since rows can not be deleted directly from a CTEs, 
-- an additional table is created including the row_num column. 
CREATE TABLE `layoffs_staging2` (
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


SELECT*
FROM layoffs_staging2
where row_num > 1;

insert into layoffs_staging2
select *,
row_number() over(
partition by industry, location, 
total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) as row_num 
from layoffs_staging;

-- Delete rows in staging2 where row_num>1
delete
FROM layoffs_staging2
where row_num > 1;




-- Step2:Standardizing data
-- Trim removes extex spaces
select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

-- Normalized the industry column:inconsistent variants
select distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- Normalized the country column:'United States' has an extra period
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select distinct country
from layoffs_staging2
order by 1;

-- Standardize the date to a consitent format
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
from  layoffs_staging2;

-- Update `date`
update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

-- Convert the `date` data type from text to date
alter table layoffs_staging2
modify column `date` DATE;



-- Step3：Null values or blank values
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Some blank values can be inferred from other fields
-- (eg.,same company but the industry field is blank)
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Use a self-join to identify records where industry is blank
-- but other matching company records contain a valid industry value.
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL OR t1.industry = ''
AND t2.industry IS NOT NULL;

-- Update industry column
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
		ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;



-- Step 4 : Remove Any Columns
-- Remoce records where all layoff-related fields are NULL
-- (these rows provide no analytic value)
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Drop the column row_num added for deduplication.
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
