DROP TABLE IF EXISTS public.client_attrs_templates;
DROP TABLE IF EXISTS public.client_attrs_requests;
DROP TABLE IF EXISTS public.request_cache;

CREATE TABLE IF NOT EXISTS public.client_attrs_templates (
    id serial PRIMARY KEY,
    flow varchar(20) NOT NULL,
    created_at timestamp DEFAULT NOW(),
    name varchar(255),
    template varchar
);

CREATE TABLE IF NOT EXISTS public.client_attrs_requests (
    id serial PRIMARY KEY,
    flow varchar(20) NOT NULL,
    filial varchar(20) NOT NULL,
    message_id varchar(50) NOT NULL,
    updated_at timestamp NOT NULL,
    request varchar
);

CREATE TABLE IF NOT EXISTS public.request_cache (
    id serial PRIMARY KEY,
    message_id varchar(255) NOT NULL,
    flow varchar(255) NOT NULL,
    filial varchar(255) NOT NULL,
    request_version int,
    id_integration varchar(255),
    id_master_system varchar(255),
    id_filial varchar(255),
    id_main_check_system varchar(255),
    filial_id varchar(255),
    request_type_id varchar(255)
);