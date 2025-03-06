-- Data cleaning
SELECT *
FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Look at Null or blank values (see if we can populate those fields)
-- 4. Remove unnecessary columns/rows (this could save time when querying your data)

-- REMOVING DUPLICATES

-- create a staging copy (so that if we make a mistake, we still have the raw data available to us)
-- setting new table to have the same fields
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- Inserting the data from layoffs to layoffs_staging
INSERT layoffs_staging
SELECT *
FROM layoffs;

-- using the staging copy, finding out which rows have duplicates (if row numer is greater than 1, it's a duplicate)
SELECT *, ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

-- using CTE to find the rows with duplicates
WITH duplicate_cte as
(
SELECT *, ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
) 
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Since we can't update a CTE, we need to make a new table WITH THAT NEW row_nums field so that where we CAN delete the rows with row_num >1
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` double DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` bigint DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

-- Actually deleting the duplicate rows
DELETE
FROM layoffs_staging2
WHERE row_num >1;

-- STANDARDIZING DATA
-- finding issues in your data, then fixing them 

-- eliminating leading white space in company name
UPDATE layoffs_staging2
SET company = TRIM(company);

-- setting 'Cryptocurreny' and 'Crypto Currency' industries to just be 'Crypto' (makes it easier later for visualization)
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- turning "United States." to "United States"
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country) -- if what you're trimming is not a white space, you can specify that it's trailing and it's a '.'
WHERE country LIKE 'United States%';

SELECT DISTINCT(country),TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;


-- turning date column, which is text rn to datetime kinda thing, to help in visualization later
SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

-- changing date column so that its type is now date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- NULL AND BLANK VALUES
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry  = '';

-- found out from previous query that one of the records without an industry has a company name of Airbnb, so we're gonna find other records containing Airbnb and try to find out what industry they're in
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- using a join to find which industry the blank cells should have based on their company name

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '') AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;
 
 -- Unable to replace the null values in the total_laid_off field because we don't know how many employees companies had before their layoffs
 -- Unable to replace the null values in the funds_raised_millions without using some sort of webscraping 
 
 
 -- REMOVING UNNECESSARY COLUMNS AND ROWS
 
 -- the results from the following query don't really give us any information in regards to layoffs at that company
 SELECT *
 FROM layoffs_staging2
 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
 
 DELETE 
 FROM layoffs_staging2
 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL; 
 
-- WE NO LONGER NEED THE ROW_NUM COLUMN
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;
 
 

