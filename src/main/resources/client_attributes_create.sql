CREATE TABLE IF NOT EXISTS public.client_attrs_templates (
    id serial PRIMARY KEY,
    flow varchar(50) NOT NULL,
    created_at timestamp,
    name varchar(255),
    template varchar
);

CREATE TABLE IF NOT EXISTS public.client_attrs_requests (
    id serial PRIMARY KEY,
    flow varchar(50) NOT NULL,
    filial varchar(20) NOT NULL,
    message_id varchar(50) NOT NULL,
    updated_at timestamp NOT NULL,
    request varchar
);