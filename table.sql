CREATE TABLE "public"."osm_nodes_event" (
   "event_type" character varying(32) NOT NULL,
   "id" bigint NOT NULL,
   "timestamp" timestamptz NOT NULL,
   "lat" double precision NOT NULL,
   "lon" double precision NOT NULL,
   "uid" int NOT NULL,
   "user" character varying(256) NOT NULL,
   "version" int NOT NULL,
   "tags" jsonb NOT NULL
) WITH (oids = false);

CREATE TABLE "public"."osm_nodes_state" (
   "id" bigint NOT NULL,
   "timestamp" timestamptz NOT NULL,
   "lat" double precision NOT NULL,
   "lon" double precision NOT NULL,
   "uid" int NOT NULL,
   "user" character varying(256) NOT NULL,
   "version" int NOT NULL,
   "tags" jsonb NOT NULL
) WITH (oids = false);

CREATE TABLE "public"."osm_sequence" (
   "seq" bigint NOT NULL,
   "timestamp" timestamptz NOT NULL
) WITH (oids = false);