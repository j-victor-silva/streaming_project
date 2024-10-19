CREATE TABLE public.raw_profiles (
	user_id text NOT NULL,
	"name" text NULL,
	phone text NULL,
	email text NULL,
	adress text NULL,
	city text NULL,
	state text NULL,
	gender text NULL,
	birthday date NULL,
	created_at timestamp NULL,
	ingestion_timestamp timestamp NOT NULL,
	message_id varchar NOT NULL
);