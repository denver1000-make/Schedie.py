export interface CancelledSchedules {
    id: number;
    timeslot_id: string;
    cancellation_type: string;
    cancelled_at: Date;
    cancelled_date: string;
    reason?: string;
    cancelled_by?: string;
}

export class CancelledSchedulesModel {
    static tableName = 'cancelled_schedules';

    static createTableQuery = `
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

    static async findAll(client: any): Promise<CancelledSchedules[]> {
        const query = `SELECT * FROM ${this.tableName} ORDER BY cancelled_at DESC`;
        const result = await client.query(query);
        return result.rows;
    }

    static async findByTimeslotId(client: any, timeslotId: string): Promise<CancelledSchedules[]> {
        const query = `SELECT * FROM ${this.tableName} WHERE timeslot_id = $1 ORDER BY cancelled_at DESC`;
        const result = await client.query(query, [timeslotId]);
        return result.rows;
    }

    static async findByTimeslotIdAndDate(client: any, timeslotId: string, cancelledDate: string): Promise<CancelledSchedules | null> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE timeslot_id = $1 AND cancelled_date = $2 
            ORDER BY cancelled_at DESC 
            LIMIT 1
        `;
        const result = await client.query(query, [timeslotId, cancelledDate]);
        return result.rows[0] || null;
    }

    static async findCancelledSchedules(client: any, date?: string): Promise<Map<string, CancelledSchedules[]>> {
        let query = `SELECT * FROM ${this.tableName}`;
        let params: any[] = [];
        
        if (date) {
            query += ` WHERE cancelled_date = $1`;
            params = [date];
        }
        
        query += ` ORDER BY cancelled_at DESC`;
        
        const result = await client.query(query, params);
        
        const cancelledMap = new Map<string, CancelledSchedules[]>();
        result.rows.forEach((cancellation: CancelledSchedules) => {
            const existing = cancelledMap.get(cancellation.timeslot_id) || [];
            existing.push(cancellation);
            cancelledMap.set(cancellation.timeslot_id, existing);
        });
        
        return cancelledMap;
    }

    static async create(client: any, data: Omit<CancelledSchedules, 'id' | 'cancelled_at'>): Promise<CancelledSchedules> {
        const query = `
            INSERT INTO ${this.tableName} (timeslot_id, cancellation_type, cancelled_date, reason, cancelled_by)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING *
        `;
        const values = [
            data.timeslot_id,
            data.cancellation_type,
            data.cancelled_date,
            data.reason,
            data.cancelled_by
        ];
        const result = await client.query(query, values);
        return result.rows[0];
    }

    static async delete(client: any, id: number): Promise<boolean> {
        const query = `DELETE FROM ${this.tableName} WHERE id = $1`;
        const result = await client.query(query, [id]);
        return result.rowCount > 0;
    }
}