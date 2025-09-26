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
exports.RunningTurnOnJobsModel = void 0;
class RunningTurnOnJobsModel {
    static findAll(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `SELECT * FROM ${this.tableName} ORDER BY created_at DESC`;
            const result = yield client.query(query);
            return result.rows;
        });
    }
    static findByTimeslotId(client, timeslotId) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `SELECT * FROM ${this.tableName} WHERE timeslot_id = $1`;
            const result = yield client.query(query, [timeslotId]);
            return result.rows[0] || null;
        });
    }
    static findRunningJobs(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `SELECT * FROM ${this.tableName} ORDER BY created_at DESC`;
            const result = yield client.query(query);
            const runningJobsMap = new Map();
            result.rows.forEach((job) => {
                runningJobsMap.set(job.timeslot_id, job);
            });
            return runningJobsMap;
        });
    }
    static create(client, data) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            INSERT INTO ${this.tableName} (timeslot_id, is_temporary)
            VALUES ($1, $2)
            RETURNING *
        `;
            const values = [data.timeslot_id, data.is_temporary];
            const result = yield client.query(query, values);
            return result.rows[0];
        });
    }
    static delete(client, timeslotId) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `DELETE FROM ${this.tableName} WHERE timeslot_id = $1`;
            const result = yield client.query(query, [timeslotId]);
            return result.rowCount > 0;
        });
    }
}
exports.RunningTurnOnJobsModel = RunningTurnOnJobsModel;
RunningTurnOnJobsModel.tableName = 'running_turn_on_jobs';
RunningTurnOnJobsModel.createTableQuery = `
        CREATE TABLE IF NOT EXISTS public.running_turn_on_jobs
        (
            id integer NOT NULL DEFAULT nextval('running_turn_on_jobs_id_seq'::regclass),
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
    `;
