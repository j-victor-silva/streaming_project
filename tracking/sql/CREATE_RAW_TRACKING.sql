CREATE TABLE public.raw_tracking (
	purchase_date date NOT NULL,
	user_id varchar NULL,
	transaction_id varchar NOT NULL,
	revenue float8 NULL,
	products_count int8 NULL,
	store_id varchar NULL,
	store_name varchar NULL,
	seller_id varchar NULL,
	seller_name varchar NULL,
	message_id varchar NOT NULL,
	ingestion_timestamp timestamp NULL
);