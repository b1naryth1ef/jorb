CREATE TABLE IF NOT EXISTS jobs (
	id uuid primary key,
	type_name text not null,
	priority int8 not null default 0,
	state text not null,
	data jsonb not null,
	depends_on text[] not null,
	scheduled_at timestamp with time zone,
	heartbeat_at timestamp with time zone,
	started_at timestamp with time zone,
	finished_at timestamp with time zone
);
