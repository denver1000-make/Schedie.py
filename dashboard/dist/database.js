"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Database = void 0;
const pg_1 = require("pg");
class Database {
    static initialize() {
        this.pool = new pg_1.Pool({
            user: 'postgres',
            password: 'postgres',
            host: 'postgres',
            port: 5432,
            database: 'schedule_db',
            max: 20,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 2000,
        });
        this.pool.on('error', (err) => {
            console.error('Unexpected error on idle client', err);
        });
        console.log('Database pool initialized');
    }
    static getClient() {
        return __awaiter(this, void 0, void 0, function* () {
            const client = yield this.pool.connect();
            return client;
        });
    }
    static query(text, params) {
        return __awaiter(this, void 0, void 0, function* () {
            const client = yield this.pool.connect();
            try {
                const result = yield client.query(text, params);
                return result;
            }
            finally {
                client.release();
            }
        });
    }
    static testConnection() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const client = yield this.pool.connect();
                yield client.query('SELECT NOW()');
                client.release();
                console.log('Database connection successful');
                return true;
            }
            catch (error) {
                console.error('Database connection failed:', error);
                return false;
            }
        });
    }
    static createTables() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const client = yield this.pool.connect();
                // Create processed_schedule_ids table
                yield client.query(`
                CREATE TABLE IF NOT EXISTS public.processed_schedule_ids
                (
                    id serial NOT NULL,
                    schedule_id text COLLATE pg_catalog."default" NOT NULL,
                    processed_at timestamp with time zone DEFAULT now(),
                    upload_date_epoch double precision,
                    slot_count integer DEFAULT 0,
                    is_temp boolean DEFAULT false,
                    topic text COLLATE pg_catalog."default",
                    CONSTRAINT processed_schedule_ids_pkey PRIMARY KEY (id),
                    CONSTRAINT processed_schedule_ids_schedule_id_key UNIQUE (schedule_id)
                );
            `);
                // Create resolved_schedule_slots table
                yield client.query(`
                CREATE TABLE IF NOT EXISTS public.resolved_schedule_slots
                (
                    id serial NOT NULL,
                    timeslot_id character varying(255) COLLATE pg_catalog."default",
                    schedule_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
                    room_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
                    day_name character varying(20) COLLATE pg_catalog."default" NOT NULL,
                    day_order integer NOT NULL,
                    start_time character varying(20) COLLATE pg_catalog."default" NOT NULL,
                    end_time character varying(20) COLLATE pg_catalog."default" NOT NULL,
                    subject character varying(200) COLLATE pg_catalog."default",
                    teacher character varying(200) COLLATE pg_catalog."default",
                    teacher_email character varying(255) COLLATE pg_catalog."default",
                    time_start_in_seconds integer NOT NULL,
                    start_date_in_seconds_epoch bigint,
                    end_date_in_seconds_epoch bigint,
                    is_temporary boolean DEFAULT false,
                    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT resolved_schedule_slots_pkey PRIMARY KEY (id)
                );
            `);
                // Create running_turn_on_jobs table
                yield client.query(`
                CREATE TABLE IF NOT EXISTS public.running_turn_on_jobs
                (
                    id serial NOT NULL,
                    timeslot_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
                    is_temporary boolean DEFAULT false,
                    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT running_turn_on_jobs_pkey PRIMARY KEY (id),
                    CONSTRAINT running_turn_on_jobs_timeslot_id_key UNIQUE (timeslot_id)
                );

                CREATE INDEX IF NOT EXISTS idx_running_jobs_is_temporary
                    ON public.running_turn_on_jobs USING btree
                    (is_temporary ASC NULLS LAST);

                CREATE INDEX IF NOT EXISTS idx_running_jobs_timeslot_id
                    ON public.running_turn_on_jobs USING btree
                    (timeslot_id COLLATE pg_catalog."default" ASC NULLS LAST);
            `);
                // Create cancelled_schedules table
                yield client.query(`
                CREATE TABLE IF NOT EXISTS public.cancelled_schedules
                (
                    id serial NOT NULL,
                    timeslot_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
                    cancellation_type character varying(50) COLLATE pg_catalog."default" NOT NULL,
                    cancelled_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
                    cancelled_date character varying(20) COLLATE pg_catalog."default" NOT NULL,
                    reason text COLLATE pg_catalog."default",
                    cancelled_by character varying(255) COLLATE pg_catalog."default",
                    CONSTRAINT cancelled_schedules_pkey PRIMARY KEY (id),
                    CONSTRAINT cancelled_schedules_timeslot_id_cancelled_date_key UNIQUE (timeslot_id, cancelled_date)
                );

                CREATE INDEX IF NOT EXISTS idx_cancelled_schedules_cancelled_at
                    ON public.cancelled_schedules USING btree
                    (cancelled_at ASC NULLS LAST);

                CREATE INDEX IF NOT EXISTS idx_cancelled_schedules_timeslot_date
                    ON public.cancelled_schedules USING btree
                    (timeslot_id COLLATE pg_catalog."default" ASC NULLS LAST, cancelled_date COLLATE pg_catalog."default" ASC NULLS LAST);
            `);
                // Note: schedule_wrappers table already exists with different structure
                // Actual table is: public.schedule_wrappers with columns:
                // schedule_id (PK), upload_date_epoch, is_temporary, is_synced_to_remote, is_from_remote, created_at, updated_at
                client.release();
                console.log('Tables created successfully');
            }
            catch (error) {
                console.error('Error creating tables:', error);
                throw error;
            }
        });
    }
    static close() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.pool) {
                yield this.pool.end();
                console.log('Database pool closed');
            }
        });
    }
}
exports.Database = Database;
