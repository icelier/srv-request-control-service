CREATE TABLE IF NOT EXISTS public.request_identifiers (
    id serial PRIMARY KEY,
    flow varchar(255) NOT NULL,
    filial varchar(255) NOT NULL,
    request_version int NOT NULL,
    id_integration varchar(255),
    id_master_system varchar(255),
    id_filial varchar(255),
    id_main_check_system varchar(255),
    filial_id varchar(255),
    request_type_id varchar(255)
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