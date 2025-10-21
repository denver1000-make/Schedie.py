export interface ResolvedScheduleSlots {
    timeslot_id: string;
    schedule_id: string;
    room_id: string;
    day_name: string;
    day_order: number;
    start_time: string;
    end_time: string;
    subject: string;
    teacher: string;
    teacher_email?: string;
    start_hour: number;
    start_minute: number;
    end_hour: number;
    end_minute: number;
    time_start_in_seconds?: number;
    start_date_in_seconds_epoch?: number;
    end_date_in_seconds_epoch?: number;
    is_temporary: boolean;
}

export class ResolvedScheduleSlotsModel {
    static tableName = 'resolved_schedule_slots';

    static createTableQuery = `
        CREATE TABLE IF NOT EXISTS public.resolved_schedule_slots (
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
            CONSTRAINT resolved_schedule_slots_pkey PRIMARY KEY (timeslot_id)
        );
    `;

    static async findAll(client: any, limit: number = 100): Promise<ResolvedScheduleSlots[]> {
        const query = `
            SELECT * FROM ${this.tableName} 
            ORDER BY day_order, time_start_in_seconds 
            LIMIT $1
        `;
        const result = await client.query(query, [limit]);
        return result.rows;
    }

    static async findById(client: any, id: number): Promise<ResolvedScheduleSlots | null> {
        const query = `SELECT * FROM ${this.tableName} WHERE id = $1`;
        const result = await client.query(query, [id]);
        return result.rows[0] || null;
    }

    static async findByScheduleId(client: any, scheduleId: string): Promise<ResolvedScheduleSlots[]> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE schedule_id = $1 
            ORDER BY day_order, time_start_in_seconds
        `;
        const result = await client.query(query, [scheduleId]);
        return result.rows;
    }

    static async findByRoomId(client: any, roomId: string): Promise<ResolvedScheduleSlots[]> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE room_id = $1 
            ORDER BY day_order, time_start_in_seconds
        `;
        const result = await client.query(query, [roomId]);
        return result.rows;
    }

    static async findByDay(client: any, dayName: string): Promise<ResolvedScheduleSlots[]> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE day_name = $1 
            ORDER BY time_start_in_seconds
        `;
        const result = await client.query(query, [dayName]);
        return result.rows;
    }

    static async findByRoomAndTime(client: any, roomId: string, startTime: string, endTime: string): Promise<ResolvedScheduleSlots | null> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE room_id = $1 AND start_time = $2 AND end_time = $3
            LIMIT 1
        `;
        const result = await client.query(query, [roomId, startTime, endTime]);
        return result.rows[0] || null;
    }

    static async create(client: any, data: ResolvedScheduleSlots): Promise<ResolvedScheduleSlots> {
        const query = `
            INSERT INTO ${this.tableName} (
                timeslot_id, schedule_id, room_id, day_name, day_order, start_time, end_time,
                subject, teacher, teacher_email, start_hour, start_minute, end_hour, end_minute,
                time_start_in_seconds, start_date_in_seconds_epoch, end_date_in_seconds_epoch, is_temporary
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            RETURNING *
        `;
        const values = [
            data.timeslot_id,
            data.schedule_id,
            data.room_id,
            data.day_name,
            data.day_order,
            data.start_time,
            data.end_time,
            data.subject,
            data.teacher,
            data.teacher_email,
            data.start_hour,
            data.start_minute,
            data.end_hour,
            data.end_minute,
            data.time_start_in_seconds,
            data.start_date_in_seconds_epoch,
            data.end_date_in_seconds_epoch,
            data.is_temporary
        ];
        const result = await client.query(query, values);
        return result.rows[0];
    }

    static async update(client: any, id: number, data: Partial<ResolvedScheduleSlots>): Promise<ResolvedScheduleSlots | null> {
        const fields = Object.keys(data).filter(key => key !== 'id' && key !== 'created_at');
        const setClause = fields.map((field, index) => `${field} = $${index + 2}`).join(', ');
        const values = [id, ...fields.map(field => (data as any)[field])];

        const query = `
            UPDATE ${this.tableName}
            SET ${setClause}
            WHERE id = $1
            RETURNING *
        `;
        const result = await client.query(query, values);
        return result.rows[0] || null;
    }

    static async delete(client: any, id: number): Promise<boolean> {
        const query = `DELETE FROM ${this.tableName} WHERE id = $1`;
        const result = await client.query(query, [id]);
        return result.rowCount > 0;
    }

    static async deleteByScheduleId(client: any, scheduleId: string): Promise<number> {
        const query = `DELETE FROM ${this.tableName} WHERE schedule_id = $1`;
        const result = await client.query(query, [scheduleId]);
        return result.rowCount;
    }

    static async getWeeklySchedule(client: any): Promise<{[key: string]: ResolvedScheduleSlots[]}> {
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
        
        const result = await client.query(query);
        const schedules = result.rows;
        
        // Group by day_name
        const weeklySchedule: {[key: string]: ResolvedScheduleSlots[]} = {
            'Monday': [],
            'Tuesday': [],
            'Wednesday': [],
            'Thursday': [],
            'Friday': [],
            'Saturday': []
        };
        
        schedules.forEach((schedule: any) => {
            const dayName = schedule.day_name;
            if (weeklySchedule[dayName]) {
                weeklySchedule[dayName].push(schedule);
            }
        });
        
        return weeklySchedule;
    }

    static async getEnhancedWeeklySchedule(client: any, scheduleId: string): Promise<{
        schedules: {[key: string]: ResolvedScheduleSlots[]},
        runningJobs: Map<string, any>,
        cancelledSchedules: Map<string, any[]>,
        roomGroups: Map<string, ResolvedScheduleSlots[]>
    }> {
        // ALWAYS require a schedule_id - get weekly schedule filtered by schedule_id ONLY
        const weeklySchedule = await this.getWeeklyScheduleByScheduleId(client, scheduleId);
        
        // Get running jobs
        const runningJobsQuery = `SELECT * FROM running_turn_on_jobs ORDER BY timeslot_id`;
        const runningResult = await client.query(runningJobsQuery);
        const runningJobs = new Map();
        runningResult.rows.forEach((job: any) => {
            runningJobs.set(job.timeslot_id, job);
        });
        
        // Get cancelled schedules (for today's date)
        const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format
        const cancelledQuery = `
            SELECT * FROM cancelled_schedules 
            WHERE cancelled_date = $1 OR cancelled_date = 'permanent'
            ORDER BY cancelled_at DESC
        `;
        const cancelledResult = await client.query(cancelledQuery, [today]);
        const cancelledSchedules = new Map();
        cancelledResult.rows.forEach((cancellation: any) => {
            const existing = cancelledSchedules.get(cancellation.timeslot_id) || [];
            existing.push(cancellation);
            cancelledSchedules.set(cancellation.timeslot_id, existing);
        });
        
        // Group schedules by room for easier management
        const roomGroups = new Map<string, ResolvedScheduleSlots[]>();
        Object.values(weeklySchedule).flat().forEach((schedule: ResolvedScheduleSlots) => {
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
    }

    static async getWeeklyScheduleByScheduleId(client: any, scheduleId: string): Promise<{[key: string]: ResolvedScheduleSlots[]}> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE schedule_id = $1
            ORDER BY day_order, time_start_in_seconds
        `;
        const result = await client.query(query, [scheduleId]);
        
        const weeklySchedule: {[key: string]: ResolvedScheduleSlots[]} = {
            'Monday': [],
            'Tuesday': [],
            'Wednesday': [],
            'Thursday': [],
            'Friday': [],
            'Saturday': []
        };

        result.rows.forEach((slot: ResolvedScheduleSlots) => {
            const dayName = slot.day_name;
            if (weeklySchedule[dayName]) {
                // Parse time to get hours and minutes
                const [startHour, startMinute] = slot.start_time.split(':').map(Number);
                const [endHour, endMinute] = slot.end_time.split(':').map(Number);
                
                const enhancedSlot = {
                    ...slot,
                    start_hour: startHour,
                    start_minute: startMinute,
                    end_hour: endHour,
                    end_minute: endMinute
                };
                
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
    }

    static async getConsolidatedWeeklySchedule(client: any, scheduleIds: string[]): Promise<{
        schedules: {[key: string]: ResolvedScheduleSlots[]},
        runningJobs: Map<string, any>,
        cancelledSchedules: Map<string, any[]>,
        roomGroups: Map<string, ResolvedScheduleSlots[]>
    }> {
        // Get resolved schedule slots for ALL provided schedule_ids
        const placeholders = scheduleIds.map((_, index) => `$${index + 1}`).join(',');
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE schedule_id IN (${placeholders})
            ORDER BY day_order, time_start_in_seconds
        `;
        const result = await client.query(query, scheduleIds);
        
        // Organize slots by day
        const weeklySchedule: {[key: string]: ResolvedScheduleSlots[]} = {
            'Monday': [],
            'Tuesday': [],
            'Wednesday': [],
            'Thursday': [],
            'Friday': [],
            'Saturday': []
        };

        result.rows.forEach((slot: ResolvedScheduleSlots) => {
            const dayName = slot.day_name;
            if (weeklySchedule[dayName]) {
                // Parse time to get hours and minutes
                const [startHour, startMinute] = slot.start_time.split(':').map(Number);
                const [endHour, endMinute] = slot.end_time.split(':').map(Number);
                
                const enhancedSlot = {
                    ...slot,
                    start_hour: startHour,
                    start_minute: startMinute,
                    end_hour: endHour,
                    end_minute: endMinute
                };
                
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
        const runningJobsQuery = `SELECT * FROM running_turn_on_jobs ORDER BY timeslot_id`;
        const runningResult = await client.query(runningJobsQuery);
        const runningJobs = new Map();
        runningResult.rows.forEach((job: any) => {
            runningJobs.set(job.timeslot_id, job);
        });
        
        // Get cancelled schedules (for today's date)
        const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format
        const cancelledQuery = `
            SELECT * FROM cancelled_schedules 
            WHERE cancelled_date = $1 OR cancelled_date = 'permanent'
            ORDER BY cancelled_at DESC
        `;
        const cancelledResult = await client.query(cancelledQuery, [today]);
        const cancelledSchedules = new Map();
        cancelledResult.rows.forEach((cancellation: any) => {
            const existing = cancelledSchedules.get(cancellation.timeslot_id) || [];
            existing.push(cancellation);
            cancelledSchedules.set(cancellation.timeslot_id, existing);
        });
        
        // Group schedules by room for easier management
        const roomGroups = new Map<string, ResolvedScheduleSlots[]>();
        Object.values(weeklySchedule).flat().forEach((schedule: ResolvedScheduleSlots) => {
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
    }

    static async getRoomWeeklySchedule(client: any, roomId: string): Promise<{[key: string]: ResolvedScheduleSlots[]}> {
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
        
        const result = await client.query(query, [roomId]);
        const schedules = result.rows;
        
        // Group by day_name
        const weeklySchedule: {[key: string]: ResolvedScheduleSlots[]} = {
            'Monday': [],
            'Tuesday': [],
            'Wednesday': [],
            'Thursday': [],
            'Friday': [],
            'Saturday': []
        };
        
        schedules.forEach((schedule: any) => {
            const dayName = schedule.day_name;
            if (weeklySchedule[dayName]) {
                weeklySchedule[dayName].push(schedule);
            }
        });
        
        return weeklySchedule;
    }
}
