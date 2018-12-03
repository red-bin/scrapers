DROP TABLE page_references ;
DROP TABLE email_addresses ;
DROP TABLE domain_runs ;
DROP TABLE pages ;

CREATE TABLE domain_runs (
  id INTEGER PRIMARY KEY,
  domain TEXT 
) ;
  
CREATE TABLE pages (
  id SERIAL PRIMARY KEY,
  run_id INTEGER,
  url TEXT NOT NULL,
  file_type TEXT,
  headers JSON,
  file_size INTEGER,
  file_location TEXT,
  md5 TEXT,
  tika_location TEXT,
  tika_md5 TEXT,
  scrape_depth INTEGER,
  http_code INTEGER,
  time TIMESTAMP
) ;

CREATE TABLE page_references (
  original_url_id INTEGER REFERENCES pages,
  reference_url_id INTEGER REFERENCES pages
) ;

CREATE TABLE email_addresses (
  id SERIAL PRIMARY KEY,
  email_address TEXT,
  url TEXT
) ;
