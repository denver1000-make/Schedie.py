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
exports.CancelledSchedulesModel = void 0;
class CancelledSchedulesModel {
    static findAll(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `SELECT * FROM ${this.tableName} ORDER BY cancelled_at DESC`;
            const result = yield client.query(query);
            return result.rows;
        });
    }
    static findByTimeslotId(client, timeslotId) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `SELECT * FROM ${this.tableName} WHERE timeslot_id = $1 ORDER BY cancelled_at DESC`;
            const result = yield client.query(query, [timeslotId]);
            return result.rows;
        });
    }
    static findByTimeslotIdAndDate(client, timeslotId, cancelledDate) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT * FROM ${this.tableName} 
            WHERE timeslot_id = $1 AND cancelled_date = $2 
            ORDER BY cancelled_at DESC 
            LIMIT 1
        `;
            const result = yield client.query(query, [timeslotId, cancelledDate]);
            return result.rows[0] || null;
        });
    }
    static findCancelledSchedules(client, date) {
        return __awaiter(this, void 0, void 0, function* () {
            let query = `SELECT * FROM ${this.tableName}`;
            let params = [];
            if (date) {
                query += ` WHERE cancelled_date = $1`;
                params = [date];
            }
            query += ` ORDER BY cancelled_at DESC`;
            const result = yield client.query(query, params);
            const cancelledMap = new Map();
            result.rows.forEach((cancellation) => {
                const existing = cancelledMap.get(cancellation.timeslot_id) || [];
                existing.push(cancellation);
                cancelledMap.set(cancellation.timeslot_id, existing);
            });
            return cancelledMap;
        });
    }
    static create(client, data) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            INSERT INTO ${this.tableName} (timeslot_id, cancellation_type, cancelled_date, reason, cancelled_by, cancellation_id, room_id, teacher_name, teacher_id, teacher_email, day_name, year, month, day_of_month, subject, start_time, end_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            RETURNING *
        `;
            const values = [
                data.timeslot_id,
                data.cancellation_type,
                data.cancelled_date,
                data.reason,
                data.cancelled_by,
                data.cancellation_id,
                data.room_id,
                data.teacher_name,
                data.teacher_id,
                data.teacher_email,
                data.day_name,
                data.year,
                data.month,
                data.day_of_month,
                data.subject,
                data.start_time,
                data.end_time
            ];
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
exports.CancelledSchedulesModel = CancelledSchedulesModel;
CancelledSchedulesModel.tableName = 'cancelled_schedules';
CancelledSchedulesModel.createTableQuery = `
        CREATE TABLE IF NOT EXISTS public.cancelled_schedules
        (
            id integer NOT NULL DEFAULT nextval('cancelled_schedules_id_seq'::regclass),
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
    `;
