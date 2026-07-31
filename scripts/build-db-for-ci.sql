-- ============================================================================
--  pgcyan integration test dataset 
-- ============================================================================

\set ON_ERROR_STOP on


-- ---------------------------------------------------------------- shop component
CREATE TABLE public.customers (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email       text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    region      text NOT NULL
);

CREATE TABLE public.products (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku         text NOT NULL UNIQUE,
    name        text NOT NULL,
    price_cents int  NOT NULL
);

CREATE TABLE public.orders (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES public.customers(id),
    placed_at   timestamptz NOT NULL DEFAULT now(),
    status      text NOT NULL
);

CREATE TABLE public.order_items (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id    bigint NOT NULL REFERENCES public.orders(id),
    product_id  bigint NOT NULL REFERENCES public.products(id),
    qty         int NOT NULL,
    unit_cents  int NOT NULL
);

-- ---------------------------------------------------------------- media component
CREATE TABLE public.authors (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    handle      text NOT NULL UNIQUE,
    joined_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.articles (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    author_id   bigint NOT NULL REFERENCES public.authors(id),
    title       text NOT NULL,
    body        text NOT NULL,
    published_at timestamptz
);

CREATE TABLE public.comments (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    article_id  bigint NOT NULL REFERENCES public.articles(id),
    author_id   bigint NOT NULL REFERENCES public.authors(id),
    body        text NOT NULL,
    posted_at   timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- 
-- 1. PARENT TABLES (Dimensions) - Dynamic FK lookup guarantees safety
-- ---------------------------------------------------------------- 
INSERT INTO public.customers (email, region)
SELECT 'cust'||g||'@example.com',
       (ARRAY['us','eu','apac','latam'])[1+floor(random()*4)]
FROM generate_series(1, 10000) g;

INSERT INTO public.products (sku, name, price_cents)
SELECT 'SKU-'||g, 'Product '||g, 100+floor(random()*9900)::int
FROM generate_series(1, 200) g;

INSERT INTO public.authors (handle)
SELECT 'author_'||g 
FROM generate_series(1, 500) g;

-- ---------------------------------------------------------------- 
-- 2. CHILD TABLES (Facts) - Dynamic FK lookup guarantees safety
-- ---------------------------------------------------------------- 
INSERT INTO public.orders (customer_id, placed_at, status)
SELECT (SELECT id FROM public.customers ORDER BY random() LIMIT 1),
       now() - (floor(random()*365)||' days')::interval,
       (ARRAY['pending','shipped','delivered','cancelled'])[1+floor(random()*4)]
FROM generate_series(1, 100000) g;

INSERT INTO public.order_items (order_id, product_id, qty, unit_cents)
SELECT (SELECT id FROM public.orders ORDER BY random() LIMIT 1),
       (SELECT id FROM public.products ORDER BY random() LIMIT 1),
       1+floor(random()*5)::int,
       100+floor(random()*9900)::int
FROM generate_series(1, 250000) g;

INSERT INTO public.articles (author_id, title, body, published_at)
SELECT (SELECT id FROM public.authors ORDER BY random() LIMIT 1),
       'Title '||g, repeat(md5(g::text), 8),
       now() - (floor(random()*200)||' days')::interval
FROM generate_series(1, 20000) g;

INSERT INTO public.comments (article_id, author_id, body)
SELECT (SELECT id FROM public.articles ORDER BY random() LIMIT 1),
       (SELECT id FROM public.authors ORDER BY random() LIMIT 1),
       repeat(md5(g::text), 2)
FROM generate_series(1, 100000) g;

-- ---------------------------------------------------------------- 
-- 3. SECONDARY INDEXES & OPTIMIZATION
-- ---------------------------------------------------------------- 
CREATE INDEX ON public.orders      (customer_id);
CREATE INDEX ON public.orders      (placed_at);
CREATE INDEX ON public.order_items (order_id);
CREATE INDEX ON public.order_items (product_id);
CREATE INDEX ON public.articles    (author_id);
CREATE INDEX ON public.comments    (article_id);
CREATE INDEX ON public.comments    (author_id);

ANALYZE;

-- ---------------------------------------------------------------- summary
SELECT schemaname||'.'||relname AS table,
       n_live_tup                AS rows,
       pg_size_pretty(pg_total_relation_size(relid)) AS size
FROM pg_stat_user_tables
WHERE schemaname IN ('shop','media')
ORDER BY pg_total_relation_size(relid) DESC;
