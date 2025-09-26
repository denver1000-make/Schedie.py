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
exports.ProcessedScheduleIdsModel = void 0;
class ProcessedScheduleIdsModel {
    static findAll(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `SELECT * FROM ${this.tableName} ORDER BY processed_at DESC`;
            const result = yield client.query(query);
            return result.rows;
        });
    }
    static findById(client, id) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `SELECT * FROM ${this.tableName} WHERE id = $1`;
            const result = yield client.query(query, [id]);
            return result.rows[0] || null;
        });
    }
    static findByScheduleId(client, scheduleId) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `SELECT * FROM ${this.tableName} WHERE schedule_id = $1`;
            const result = yield client.query(query, [scheduleId]);
            return result.rows[0] || null;
        });
    }
    static create(client, data) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            INSERT INTO ${this.tableName} (schedule_id, upload_date_epoch, slot_count, is_temp, topic)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING *
        `;
            const values = [
                data.schedule_id,
                data.upload_date_epoch,
                data.slot_count || 0,
                data.is_temp || false,
                data.topic
            ];
            const result = yield client.query(query, values);
            return result.rows[0];
        });
    }
    static update(client, id, data) {
        return __awaiter(this, void 0, void 0, function* () {
            const fields = Object.keys(data).filter(key => key !== 'id');
            const setClause = fields.map((field, index) => `${field} = $${index + 2}`).join(', ');
            const values = [id, ...fields.map(field => data[field])];
            const query = `
            UPDATE ${this.tableName}
            SET ${setClause}
            WHERE id = $1
            RETURNING *
        `;
            const result = yield client.query(query, values);
            return result.rows[0] || null;
        });
    }
    static delete(client, id) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `DELETE FROM ${this.tableName} WHERE id = $1`;
            const result = yield client.query(query, [id]);
            return result.rowCount > 0;
        });
    }
}
exports.ProcessedScheduleIdsModel = ProcessedScheduleIdsModel;
ProcessedScheduleIdsModel.tableName = 'processed_schedule_ids';
ProcessedScheduleIdsModel.createTableQuery = `
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
    `;
