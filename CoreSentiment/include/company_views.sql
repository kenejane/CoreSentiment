WITH company_views AS (
SELECT 
    CASE 
    WHEN page_title ILIKE '%amazon%' OR domain_code ILIKE '%amazon%' THEN 'Amazon'
    WHEN page_title ILIKE '%apple%' OR domain_code ILIKE '%apple%' THEN 'Apple'
    WHEN page_title ILIKE '%facebook%' OR domain_code ILIKE '%facebook%' THEN 'Facebook'
    WHEN page_title ILIKE '%google%' OR domain_code ILIKE '%google%' THEN 'Google'
    WHEN page_title ILIKE '%microsoft%' OR domain_code ILIKE '%microsoft%' THEN 'Microsoft'
    ELSE 'Other'
    END as company,
    SUM(count_views) as total_views
    FROM pageviews
    WHERE 
        (page_title ILIKE '%amazon%' OR domain_code ILIKE '%amazon%' OR
        page_title ILIKE '%apple%' OR domain_code ILIKE '%apple%' OR
        page_title ILIKE '%facebook%' OR domain_code ILIKE '%facebook%' OR
        page_title ILIKE '%google%' OR domain_code ILIKE '%google%' OR
        page_title ILIKE '%microsoft%' OR domain_code ILIKE '%microsoft%')
        GROUP BY 1
)    
    SELECT 
    company,
    total_views
    FROM company_views
    WHERE company != 'Other'
    ORDER BY total_views DESC
    LIMIT 1;