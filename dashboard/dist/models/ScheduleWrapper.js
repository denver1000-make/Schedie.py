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
exports.ScheduleWrapperModel = void 0;
class ScheduleWrapperModel {
    static findAll(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `SELECT * FROM ${this.tableName} ORDER BY upload_date_epoch DESC`;
            const result = yield client.query(query);
            return result.rows;
        });
    }
    static findMostRecent(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT * FROM ${this.tableName} 
            ORDER BY upload_date_epoch DESC 
            LIMIT 1
        `;
            const result = yield client.query(query, []);
            return result.rows[0] || null;
        });
    }
    static findMostRecentPermanent(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT * FROM ${this.tableName} 
            WHERE is_temporary = false AND in_use = true
            ORDER BY upload_date_epoch DESC 
            LIMIT 1
        `;
            const result = yield client.query(query);
            return result.rows[0] || null;
        });
    }
    static findActiveTemporary(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT * FROM ${this.tableName} 
            WHERE is_temporary = true 
            ORDER BY upload_date_epoch DESC
        `;
            const result = yield client.query(query);
            return result.rows;
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
            INSERT INTO ${this.tableName} (schedule_id, upload_date_epoch, is_temporary, is_synced_to_remote, is_from_remote, in_use)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
        `;
            const values = [
                data.schedule_id,
                data.upload_date_epoch,
                data.is_temporary,
                data.is_synced_to_remote,
                data.is_from_remote,
                data.in_use
            ];
            const result = yield client.query(query, values);
            return result.rows[0];
        });
    }
    static update(client, scheduleId, data) {
        return __awaiter(this, void 0, void 0, function* () {
            const fields = Object.keys(data).filter(key => key !== 'schedule_id');
            const setClause = fields.map((field, index) => `${field} = $${index + 2}`).join(', ');
            const values = [scheduleId, ...fields.map(field => data[field])];
            const query = `
            UPDATE ${this.tableName}
            SET ${setClause}
            WHERE schedule_id = $1
            RETURNING *
        `;
            const result = yield client.query(query, values);
            return result.rows[0] || null;
        });
    }
    static delete(client, scheduleId) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `DELETE FROM ${this.tableName} WHERE schedule_id = $1`;
            const result = yield client.query(query, [scheduleId]);
            return result.rowCount > 0;
        });
    }
}
exports.ScheduleWrapperModel = ScheduleWrapperModel;
ScheduleWrapperModel.tableName = 'schedule_wrappers';
ScheduleWrapperModel.createTableQuery = `
        CREATE TABLE IF NOT EXISTS public.schedule_wrappers (
            schedule_id character varying NOT NULL,
            upload_date_epoch double precision NOT NULL,
            is_temporary boolean NOT NULL,
            is_synced_to_remote boolean NOT NULL,
            is_from_remote boolean NOT NULL,
            in_use boolean NOT NULL,
            CONSTRAINT schedule_wrappers_pkey PRIMARY KEY (schedule_id)
        );
    `;
