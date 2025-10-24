CREATE TABLE IF NOT EXISTS pageviews (
            domain_code VARCHAR(50),
            page_title VARCHAR(500),
            count_views INTEGER,
            total_response_size INTEGER,
            date_hour TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_pageviews_domain ON pageviews(domain_code);
        CREATE INDEX IF NOT EXISTS idx_pageviews_title ON pageviews(page_title);
        CREATE INDEX IF NOT EXISTS idx_pageviews_date ON pageviews(date_hour);
        