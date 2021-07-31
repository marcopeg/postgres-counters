
module.exports = {
  reset: async (db) => {
    await db.query('DROP SCHEMA IF EXISTS "pg-counters" CASCADE;');
    await db.query('DROP SCHEMA IF EXISTS "postgres-counters" CASCADE;');
    await db.query('CREATE SCHEMA IF NOT EXISTS "postgres-counters";');
  },
  create: async (db) => {
    await db.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";');

    await db.query(`
      CREATE TABLE IF NOT EXISTS "postgres-counters"."logs" (
        "id" UUID DEFAULT uuid_generate_v1() PRIMARY KEY,
        "created_at" TIMESTAMP WITH TIME ZONE DEFAULT now(),
        "scope" CHARACTER VARYING(40) NOT NULL,
        "value" BIGINT,
        "is_reset" BOOLEAN DEFAULT false
      );

      CREATE TABLE IF NOT EXISTS "postgres-counters"."values" (
        "scope" CHARACTER VARYING(40) PRIMARY KEY,
        "value" BIGINT DEFAULT 0,
        "created_at" TIMESTAMP WITH TIME ZONE DEFAULT now(),
        "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT now()
      );

      CREATE TABLE IF NOT EXISTS "postgres-counters"."logs_test" (
        "id" UUID DEFAULT uuid_generate_v1() PRIMARY KEY,
        "created_at" TIMESTAMP WITH TIME ZONE DEFAULT now(),
        "scope" CHARACTER VARYING(40) NOT NULL,
        "value" BIGINT,
        "is_reset" BOOLEAN DEFAULT false
      );

      CREATE TABLE IF NOT EXISTS "postgres-counters"."values_test" (
        "scope" CHARACTER VARYING(40) PRIMARY KEY,
        "value" BIGINT DEFAULT 0,
        "created_at" TIMESTAMP WITH TIME ZONE DEFAULT now(),
        "updated_at" TIMESTAMP WITH TIME ZONE DEFAULT now()
      );

      DROP FUNCTION IF EXISTS "postgres-counters"."log_pack"(integer);
      CREATE OR REPLACE FUNCTION "postgres-counters"."log_pack"(
        PAR_limit INTEGER,
        OUT affected_rows INTEGER
      ) AS $$
      DECLARE
        VAR_r RECORD;
      BEGIN
        -- select a range of records to squeeze
        CREATE TEMP TABLE "postgres_counters_logs_pack_source"
        ON COMMIT DROP
        AS
          SELECT * FROM "postgres-counters"."logs" 
          ORDER BY "created_at" ASC
          FOR UPDATE
          LIMIT PAR_limit;

        -- this copy of the temporary table is being used to store the records to delete
        CREATE TEMP TABLE "postgres_counters_logs_pack_delete"
        ON COMMIT DROP
        AS SELECT "id" FROM "postgres_counters_logs_pack_source";

        -- handle reset logs
        -- need to find the last reset event for each scope
        -- then run the upsert query
        -- then remove any previous increments or reset logs from the temporary table
        FOR VAR_r IN
          INSERT INTO "postgres-counters"."values" AS "target" ("scope", "value", "created_at", "updated_at")
            WITH "values" AS (
              SELECT
              "scope", "value", "created_at", "created_at" AS "updated_at",
              ROW_NUMBER() OVER (PARTITION BY "scope" ORDER BY "created_at" DESC) AS "partition"
              FROM "postgres_counters_logs_pack_source" 
              WHERE "is_reset" IS TRUE
              ORDER BY "created_at" ASC
            )
            SELECT "scope", "value", "created_at", "updated_at" FROM "values" WHERE "partition" = 1
          ON CONFLICT ON CONSTRAINT "values_pkey" DO
          UPDATE SET 
              "value" = EXCLUDED."value",
              "updated_at" = EXCLUDED."created_at"
          RETURNING *
        LOOP
          DELETE FROM "postgres_counters_logs_pack_source"
          WHERE "scope" = VAR_r."scope"
            AND "created_at" <= VAR_r."updated_at";
        END LOOP;

        TRUNCATE "postgres-counters"."logs_test";
        TRUNCATE "postgres-counters"."values_test";
        INSERT INTO "postgres-counters"."logs_test" SELECT * FROM "postgres_counters_logs_pack_source";
        INSERT INTO "postgres-counters"."values_test" SELECT * FROM "postgres-counters"."values";
        
        -- squeeze increments by counter
        INSERT INTO "postgres-counters"."values" AS "target" ("scope", "value")
          SELECT "scope", SUM("value") AS "value" 
          FROM "postgres_counters_logs_pack_source"
          GROUP BY "scope"
        ON CONFLICT ON CONSTRAINT "values_pkey" DO
          UPDATE SET 
            "value" = "target"."value" + EXCLUDED."value",
            "updated_at" = NOW();

        -- drop all the squeezed records
        DELETE FROM "postgres-counters"."logs" WHERE "id" IN
        (SELECT "id" FROM "postgres_counters_logs_pack_delete");
        GET DIAGNOSTICS affected_rows := ROW_COUNT;

      END; $$
      LANGUAGE plpgsql;
    `);
  },
  log: async (db, scope = '*', value = 0) => {
    const res = await db.query(`
      INSERT INTO "postgres-counters"."logs"
      ("scope", "value")
      VALUES
      ('${scope}', ${value})
      returning "id"
    `)

    return res.rows[0].id
  },
  set: async (db, scope = '*', value = 0) => {
    const res = await db.query(`
      INSERT INTO "postgres-counters"."logs"
      ("scope", "value", "is_reset")
      VALUES
      ('${scope}', ${value}, true)
      returning "id"
    `)

    return res.rows[0].id
  },
  pack: async (db, limit = 50) => {
    const res = await db.query(`
      SELECT * FROM "postgres-counters"."log_pack"(${limit}); 
    `)

    return res.rows[0].affected_rows;
  },
  getValue: async (db, scope = '*') => {
    const res = await db.query(`
      SELECT * FROM "postgres-counters"."values"
      WHERE "scope" = '${scope}';
    `)

    return Number(res.rows[0].value);
  }
};
