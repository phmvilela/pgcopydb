-- ============================================================================
--  pgcyan development dataset  —  build.sql
--  Recreate a clean test source from scratch.  No flags, no variables.
--    psql "host=<source> port=5432 dbname=postgres user=demo sslmode=require" -f build.sql
--
--  ~600 MB.  Two independent FK components (shop, media). Every table has a PK
--  and an identity sequence. Happy path: no keyless tables, no large objects.
--  Bump the three GENERATE_SERIES multipliers at the bottom for more data.
-- ============================================================================

\set ON_ERROR_STOP on

DROP SCHEMA IF EXISTS shop  CASCADE;
DROP SCHEMA IF EXISTS media CASCADE;
CREATE SCHEMA shop;
CREATE SCHEMA media;

-- ---------------------------------------------------------------- shop component
CREATE TABLE shop.customers (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email       text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    region      text NOT NULL
);

CREATE TABLE shop.products (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku         text NOT NULL UNIQUE,
    name        text NOT NULL,
    price_cents int  NOT NULL
);

CREATE TABLE shop.orders (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES shop.customers(id),
    placed_at   timestamptz NOT NULL DEFAULT now(),
    status      text NOT NULL
);

CREATE TABLE shop.order_items (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id    bigint NOT NULL REFERENCES shop.orders(id),
    product_id  bigint NOT NULL REFERENCES shop.products(id),
    qty         int NOT NULL,
    unit_cents  int NOT NULL
);

-- ---------------------------------------------------------------- media component
CREATE TABLE media.authors (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    handle      text NOT NULL UNIQUE,
    joined_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE media.articles (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    author_id   bigint NOT NULL REFERENCES media.authors(id),
    title       text NOT NULL,
    body        text NOT NULL,
    published_at timestamptz
);

CREATE TABLE media.comments (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    article_id  bigint NOT NULL REFERENCES media.articles(id),
    author_id   bigint NOT NULL REFERENCES media.authors(id),
    body        text NOT NULL,
    posted_at   timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- data
-- Dimensions first (parents), then facts (children). Multipliers set the size.
INSERT INTO shop.customers (email, region)
SELECT 'cust'||g||'@example.com',
       (ARRAY['us','eu','apac','latam'])[1+floor(random()*4)]
FROM generate_series(1, 100000) g;

INSERT INTO shop.products (sku, name, price_cents)
SELECT 'SKU-'||g, 'Product '||g, 100+floor(random()*9900)::int
FROM generate_series(1, 2000) g;

INSERT INTO media.authors (handle)
SELECT 'author_'||g FROM generate_series(1, 5000) g;

INSERT INTO shop.orders (customer_id, placed_at, status)
SELECT 1+floor(random()*100000)::bigint,
       now() - (floor(random()*365)||' days')::interval,
       (ARRAY['pending','shipped','delivered','cancelled'])[1+floor(random()*4)]
FROM generate_series(1, 1000000) g;

INSERT INTO shop.order_items (order_id, product_id, qty, unit_cents)
SELECT 1+floor(random()*1000000)::bigint,
       1+floor(random()*2000)::bigint,
       1+floor(random()*5)::int,
       100+floor(random()*9900)::int
FROM generate_series(1, 2500000) g;

INSERT INTO media.articles (author_id, title, body, published_at)
SELECT 1+floor(random()*5000)::bigint,
       'Title '||g, repeat(md5(g::text), 8),
       now() - (floor(random()*200)||' days')::interval
FROM generate_series(1, 200000) g;

INSERT INTO media.comments (article_id, author_id, body)
SELECT 1+floor(random()*200000)::bigint,
       1+floor(random()*5000)::bigint,
       repeat(md5(g::text), 2)
FROM generate_series(1, 1000000) g;

-- ---------------------------------------------------------------- secondary indexes
CREATE INDEX ON shop.orders      (customer_id);
CREATE INDEX ON shop.orders      (placed_at);
CREATE INDEX ON shop.order_items (order_id);
CREATE INDEX ON shop.order_items (product_id);
CREATE INDEX ON media.articles   (author_id);
CREATE INDEX ON media.comments   (article_id);
CREATE INDEX ON media.comments   (author_id);

ANALYZE;

-- ---------------------------------------------------------------- summary
SELECT schemaname||'.'||relname AS table,
       n_live_tup                AS rows,
       pg_size_pretty(pg_total_relation_size(relid)) AS size
FROM pg_stat_user_tables
WHERE schemaname IN ('shop','media')
ORDER BY pg_total_relation_size(relid) DESC;
