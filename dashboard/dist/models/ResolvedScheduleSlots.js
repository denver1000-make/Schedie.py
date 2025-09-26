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
exports.ResolvedScheduleSlotsModel = void 0;
class ResolvedScheduleSlotsModel {
    static findAll(client_1) {
        return __awaiter(this, arguments, void 0, function* (client, limit = 100) {
            const query = `
            SELECT * FROM ${this.tableName} 
            ORDER BY day_order, time_start_in_seconds 
            LIMIT $1
        `;
            const result = yield client.query(query, [limit]);
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
            const query = `
            SELECT * FROM ${this.tableName} 
            WHERE schedule_id = $1 
            ORDER BY day_order, time_start_in_seconds
        `;
            const result = yield client.query(query, [scheduleId]);
            return result.rows;
        });
    }
    static findByRoomId(client, roomId) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT * FROM ${this.tableName} 
            WHERE room_id = $1 
            ORDER BY day_order, time_start_in_seconds
        `;
            const result = yield client.query(query, [roomId]);
            return result.rows;
        });
    }
    static findByDay(client, dayName) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT * FROM ${this.tableName} 
            WHERE day_name = $1 
            ORDER BY time_start_in_seconds
        `;
            const result = yield client.query(query, [dayName]);
            return result.rows;
        });
    }
    static findByRoomAndTime(client, roomId, startTime, endTime) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT * FROM ${this.tableName} 
            WHERE room_id = $1 AND start_time = $2 AND end_time = $3
            LIMIT 1
        `;
            const result = yield client.query(query, [roomId, startTime, endTime]);
            return result.rows[0] || null;
        });
    }
    static create(client, data) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            INSERT INTO ${this.tableName} (
                schedule_id, room_id, day_name, day_order, start_time, end_time,
                subject, teacher, teacher_email, time_start_in_seconds,
                start_date_in_seconds_epoch, end_date_in_seconds_epoch
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING *
        `;
            const values = [
                data.schedule_id,
                data.room_id,
                data.day_name,
                data.day_order,
                data.start_time,
                data.end_time,
                data.subject,
                data.teacher,
                data.teacher_email,
                data.time_start_in_seconds,
                data.start_date_in_seconds_epoch,
                data.end_date_in_seconds_epoch
            ];
            const result = yield client.query(query, values);
            return result.rows[0];
        });
    }
    static update(client, id, data) {
        return __awaiter(this, void 0, void 0, function* () {
            const fields = Object.keys(data).filter(key => key !== 'id' && key !== 'created_at');
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
    static deleteByScheduleId(client, scheduleId) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `DELETE FROM ${this.tableName} WHERE schedule_id = $1`;
            const result = yield client.query(query, [scheduleId]);
            return result.rowCount;
        });
    }
    static getWeeklySchedule(client) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT *,
                CASE 
                    WHEN is_temporary = true AND start_date_in_seconds_epoch IS NOT NULL THEN
                        EXTRACT(HOUR FROM to_timestamp(start_date_in_seconds_epoch)) * 3600 + 
                        EXTRACT(MINUTE FROM to_timestamp(start_date_in_seconds_epoch)) * 60
                    ELSE 
                        COALESCE(time_start_in_seconds, start_hour * 3600 + start_minute * 60)
                END as calculated_start_seconds
            FROM ${this.tableName} 
            ORDER BY day_order, calculated_start_seconds
        `;
            const result = yield client.query(query);
            const schedules = result.rows;
            // Group by day_name
            const weeklySchedule = {
                'Monday': [],
                'Tuesday': [],
                'Wednesday': [],
                'Thursday': [],
                'Friday': [],
                'Saturday': []
            };
            schedules.forEach((schedule) => {
                const dayName = schedule.day_name;
                if (weeklySchedule[dayName]) {
                    weeklySchedule[dayName].push(schedule);
                }
            });
            return weeklySchedule;
        });
    }
    static getEnhancedWeeklySchedule(client, scheduleId) {
        return __awaiter(this, void 0, void 0, function* () {
            // ALWAYS require a schedule_id - get weekly schedule filtered by schedule_id ONLY
            const weeklySchedule = yield this.getWeeklyScheduleByScheduleId(client, scheduleId);
            // Get running jobs
            const runningJobsQuery = `SELECT * FROM running_turn_on_jobs ORDER BY created_at DESC`;
            const runningResult = yield client.query(runningJobsQuery);
            const runningJobs = new Map();
            runningResult.rows.forEach((job) => {
                runningJobs.set(job.timeslot_id, job);
            });
            // Get cancelled schedules (for today's date)
            const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format
            const cancelledQuery = `
            SELECT * FROM cancelled_schedules 
            WHERE cancelled_date = $1 OR cancelled_date = 'permanent'
            ORDER BY cancelled_at DESC
        `;
            const cancelledResult = yield client.query(cancelledQuery, [today]);
            const cancelledSchedules = new Map();
            cancelledResult.rows.forEach((cancellation) => {
                const existing = cancelledSchedules.get(cancellation.timeslot_id) || [];
                existing.push(cancellation);
                cancelledSchedules.set(cancellation.timeslot_id, existing);
            });
            // Group schedules by room for easier management
            const roomGroups = new Map();
            Object.values(weeklySchedule).flat().forEach((schedule) => {
                const roomSchedules = roomGroups.get(schedule.room_id) || [];
                roomSchedules.push(schedule);
                roomGroups.set(schedule.room_id, roomSchedules);
            });
            return {
                schedules: weeklySchedule,
                runningJobs,
                cancelledSchedules,
                roomGroups
            };
        });
    }
    static getWeeklyScheduleByScheduleId(client, scheduleId) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT * FROM ${this.tableName} 
            WHERE schedule_id = $1
            ORDER BY day_order, time_start_in_seconds
        `;
            const result = yield client.query(query, [scheduleId]);
            const weeklySchedule = {
                'Monday': [],
                'Tuesday': [],
                'Wednesday': [],
                'Thursday': [],
                'Friday': [],
                'Saturday': []
            };
            result.rows.forEach((slot) => {
                const dayName = slot.day_name;
                if (weeklySchedule[dayName]) {
                    // Parse time to get hours and minutes
                    const [startHour, startMinute] = slot.start_time.split(':').map(Number);
                    const [endHour, endMinute] = slot.end_time.split(':').map(Number);
                    const enhancedSlot = Object.assign(Object.assign({}, slot), { start_hour: startHour, start_minute: startMinute, end_hour: endHour, end_minute: endMinute });
                    weeklySchedule[dayName].push(enhancedSlot);
                }
            });
            // Sort each day's schedules by start time
            Object.keys(weeklySchedule).forEach(day => {
                weeklySchedule[day].sort((a, b) => {
                    if (a.start_hour !== b.start_hour) {
                        return a.start_hour - b.start_hour;
                    }
                    return a.start_minute - b.start_minute;
                });
            });
            return weeklySchedule;
        });
    }
    static getConsolidatedWeeklySchedule(client, scheduleIds) {
        return __awaiter(this, void 0, void 0, function* () {
            // Get resolved schedule slots for ALL provided schedule_ids
            const placeholders = scheduleIds.map((_, index) => `$${index + 1}`).join(',');
            const query = `
            SELECT * FROM ${this.tableName} 
            WHERE schedule_id IN (${placeholders})
            ORDER BY day_order, time_start_in_seconds
        `;
            const result = yield client.query(query, scheduleIds);
            // Organize slots by day
            const weeklySchedule = {
                'Monday': [],
                'Tuesday': [],
                'Wednesday': [],
                'Thursday': [],
                'Friday': [],
                'Saturday': []
            };
            result.rows.forEach((slot) => {
                const dayName = slot.day_name;
                if (weeklySchedule[dayName]) {
                    // Parse time to get hours and minutes
                    const [startHour, startMinute] = slot.start_time.split(':').map(Number);
                    const [endHour, endMinute] = slot.end_time.split(':').map(Number);
                    const enhancedSlot = Object.assign(Object.assign({}, slot), { start_hour: startHour, start_minute: startMinute, end_hour: endHour, end_minute: endMinute });
                    weeklySchedule[dayName].push(enhancedSlot);
                }
            });
            // Sort each day's schedules by start time, temporary schedules come after permanent ones at same time
            Object.keys(weeklySchedule).forEach(day => {
                weeklySchedule[day].sort((a, b) => {
                    if (a.start_hour !== b.start_hour) {
                        return a.start_hour - b.start_hour;
                    }
                    if (a.start_minute !== b.start_minute) {
                        return a.start_minute - b.start_minute;
                    }
                    // If same time, permanent schedules come first, then temporary
                    if (a.is_temporary !== b.is_temporary) {
                        return a.is_temporary ? 1 : -1;
                    }
                    return 0;
                });
            });
            // Get running jobs
            const runningJobsQuery = `SELECT * FROM running_turn_on_jobs ORDER BY created_at DESC`;
            const runningResult = yield client.query(runningJobsQuery);
            const runningJobs = new Map();
            runningResult.rows.forEach((job) => {
                runningJobs.set(job.timeslot_id, job);
            });
            // Get cancelled schedules (for today's date)
            const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format
            const cancelledQuery = `
            SELECT * FROM cancelled_schedules 
            WHERE cancelled_date = $1 OR cancelled_date = 'permanent'
            ORDER BY cancelled_at DESC
        `;
            const cancelledResult = yield client.query(cancelledQuery, [today]);
            const cancelledSchedules = new Map();
            cancelledResult.rows.forEach((cancellation) => {
                const existing = cancelledSchedules.get(cancellation.timeslot_id) || [];
                existing.push(cancellation);
                cancelledSchedules.set(cancellation.timeslot_id, existing);
            });
            // Group schedules by room for easier management
            const roomGroups = new Map();
            Object.values(weeklySchedule).flat().forEach((schedule) => {
                const roomSchedules = roomGroups.get(schedule.room_id) || [];
                roomSchedules.push(schedule);
                roomGroups.set(schedule.room_id, roomSchedules);
            });
            return {
                schedules: weeklySchedule,
                runningJobs,
                cancelledSchedules,
                roomGroups
            };
        });
    }
    static getRoomWeeklySchedule(client, roomId) {
        return __awaiter(this, void 0, void 0, function* () {
            const query = `
            SELECT *,
                CASE 
                    WHEN is_temporary = true AND start_date_in_seconds_epoch IS NOT NULL THEN
                        EXTRACT(HOUR FROM to_timestamp(start_date_in_seconds_epoch)) * 3600 + 
                        EXTRACT(MINUTE FROM to_timestamp(start_date_in_seconds_epoch)) * 60
                    ELSE 
                        COALESCE(time_start_in_seconds, start_hour * 3600 + start_minute * 60)
                END as calculated_start_seconds
            FROM ${this.tableName} 
            WHERE room_id = $1
            ORDER BY day_order, calculated_start_seconds
        `;
            const result = yield client.query(query, [roomId]);
            const schedules = result.rows;
            // Group by day_name
            const weeklySchedule = {
                'Monday': [],
                'Tuesday': [],
                'Wednesday': [],
                'Thursday': [],
                'Friday': [],
                'Saturday': []
            };
            schedules.forEach((schedule) => {
                const dayName = schedule.day_name;
                if (weeklySchedule[dayName]) {
                    weeklySchedule[dayName].push(schedule);
                }
            });
            return weeklySchedule;
        });
    }
}
exports.ResolvedScheduleSlotsModel = ResolvedScheduleSlotsModel;
ResolvedScheduleSlotsModel.tableName = 'resolved_schedule_slots';
ResolvedScheduleSlotsModel.createTableQuery = `
        CREATE TABLE IF NOT EXISTS public.resolved_schedule_slots
        (
            id integer NOT NULL DEFAULT nextval('resolved_schedule_slots_id_seq'::regclass),
            timeslot_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
            schedule_id character varying(255) COLLATE pg_catalog."default" NOT NULL,
            room_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
            day_name character varying(20) COLLATE pg_catalog."default" NOT NULL,
            day_order integer NOT NULL,
            start_time character varying(20) COLLATE pg_catalog."default" NOT NULL,
            end_time character varying(20) COLLATE pg_catalog."default" NOT NULL,
            subject character varying(200) COLLATE pg_catalog."default",
            teacher character varying(200) COLLATE pg_catalog."default",
            teacher_email character varying(255) COLLATE pg_catalog."default",
            start_hour integer NOT NULL DEFAULT 0,
            start_minute integer NOT NULL DEFAULT 0,
            end_hour integer NOT NULL DEFAULT 0,
            end_minute integer NOT NULL DEFAULT 0,
            time_start_in_seconds integer,
            start_date_in_seconds_epoch bigint,
            end_date_in_seconds_epoch bigint,
            is_temporary boolean DEFAULT false,
            created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT resolved_schedule_slots_pkey PRIMARY KEY (id)
        );
    `;
