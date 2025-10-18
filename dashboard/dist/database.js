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
                console.log('Migrating to new schema - dropping old tables if they exist...');
                // Drop old tables to migrate to new schema
                yield client.query(`DROP TABLE IF EXISTS processed_schedule_ids CASCADE`);
                yield client.query(`DROP TABLE IF EXISTS resolved_schedule_slots CASCADE`);
                yield client.query(`DROP TABLE IF EXISTS running_turn_on_jobs CASCADE`);
                yield client.query(`DROP TABLE IF EXISTS cancelled_schedules CASCADE`);
                yield client.query(`DROP TABLE IF EXISTS schedule_wrapper CASCADE`);
                console.log('Creating new schema tables...');
                // Create schedule_wrappers table (matches new schema)
                yield client.query(`
                CREATE TABLE IF NOT EXISTS public.schedule_wrappers (
                    schedule_id character varying NOT NULL,
                    upload_date_epoch double precision NOT NULL,
                    is_temporary boolean NOT NULL,
                    is_synced_to_remote boolean NOT NULL,
                    is_from_remote boolean NOT NULL,
                    in_use boolean NOT NULL,
                    CONSTRAINT schedule_wrappers_pkey PRIMARY KEY (schedule_id)
                );
            `);
                // Create resolved_schedule_slots_v2 table (matches new schema)
                yield client.query(`
                CREATE TABLE IF NOT EXISTS public.resolved_schedule_slots_v2 (
                    timeslot_id character varying NOT NULL,
                    schedule_id character varying NOT NULL,
                    room_id character varying NOT NULL,
                    day_name character varying NOT NULL,
                    day_order integer NOT NULL,
                    start_time character varying NOT NULL,
                    end_time character varying NOT NULL,
                    subject character varying NOT NULL,
                    teacher character varying NOT NULL,
                    teacher_email character varying,
                    start_hour integer NOT NULL,
                    start_minute integer NOT NULL,
                    end_hour integer NOT NULL,
                    end_minute integer NOT NULL,
                    time_start_in_seconds integer,
                    start_date_in_seconds_epoch double precision,
                    end_date_in_seconds_epoch double precision,
                    is_temporary boolean NOT NULL,
                    CONSTRAINT resolved_schedule_slots_v2_pkey PRIMARY KEY (timeslot_id)
                );
            `);
                // Create cancelled_schedules table (matches new schema)
                yield client.query(`
                CREATE TABLE IF NOT EXISTS public.cancelled_schedules (
                    timeslot_id character varying(255) NOT NULL,
                    cancellation_type character varying(50) NOT NULL,
                    cancelled_at timestamp without time zone NOT NULL,
                    cancelled_date character varying(20) NOT NULL,
                    reason text,
                    cancelled_by character varying(255),
                    cancellation_id character varying(255),
                    room_id character varying(50),
                    teacher_name character varying(255),
                    teacher_id character varying(255),
                    teacher_email character varying(255),
                    day_name character varying(20),
                    year integer,
                    month integer,
                    day_of_month integer,
                    subject character varying(255),
                    start_time character varying(20),
                    end_time character varying(20),
                    CONSTRAINT cancelled_schedules_pkey PRIMARY KEY (timeslot_id)
                );
            `);
                // Create running_turn_on_jobs table (matches new schema)
                yield client.query(`
                CREATE TABLE IF NOT EXISTS public.running_turn_on_jobs (
                    timeslot_id character varying NOT NULL,
                    is_temporary boolean NOT NULL,
                    CONSTRAINT running_turn_on_jobs_pkey PRIMARY KEY (timeslot_id)
                );
            `);
                // Create foreign key constraints (with proper error handling for existing constraints)
                try {
                    yield client.query(`
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'resolved_schedule_slots_v2_schedule_id_fkey'
                        ) THEN
                            ALTER TABLE ONLY public.resolved_schedule_slots_v2
                            ADD CONSTRAINT resolved_schedule_slots_v2_schedule_id_fkey 
                            FOREIGN KEY (schedule_id) REFERENCES public.schedule_wrappers(schedule_id);
                        END IF;
                    END $$;
                `);
                }
                catch (err) {
                    console.log('Note: Foreign key constraint resolved_schedule_slots_v2_schedule_id_fkey handling completed');
                }
                try {
                    yield client.query(`
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'cancelled_schedules_timeslot_id_fkey'
                        ) THEN
                            ALTER TABLE ONLY public.cancelled_schedules
                            ADD CONSTRAINT cancelled_schedules_timeslot_id_fkey 
                            FOREIGN KEY (timeslot_id) REFERENCES public.resolved_schedule_slots_v2(timeslot_id);
                        END IF;
                    END $$;
                `);
                }
                catch (err) {
                    console.log('Note: Foreign key constraint cancelled_schedules_timeslot_id_fkey handling completed');
                }
                try {
                    yield client.query(`
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.table_constraints 
                            WHERE constraint_name = 'running_turn_on_jobs_timeslot_id_fkey'
                        ) THEN
                            ALTER TABLE ONLY public.running_turn_on_jobs
                            ADD CONSTRAINT running_turn_on_jobs_timeslot_id_fkey 
                            FOREIGN KEY (timeslot_id) REFERENCES public.resolved_schedule_slots_v2(timeslot_id);
                        END IF;
                    END $$;
                `);
                }
                catch (err) {
                    console.log('Note: Foreign key constraint running_turn_on_jobs_timeslot_id_fkey handling completed');
                }
                // Create indexes
                yield client.query(`
                CREATE INDEX IF NOT EXISTS idx_cancelled_schedules_cancelled_at 
                ON public.cancelled_schedules USING btree (cancelled_at);
            `);
                client.release();
                console.log('New schema tables created successfully');
            }
            catch (error) {
                console.log('Database setup completed with warnings:', error.message);
                // Don't throw for constraint errors - they're expected on subsequent runs
                if (!error.message.includes('already exists') && !error.message.includes('constraint')) {
                    throw error;
                }
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
