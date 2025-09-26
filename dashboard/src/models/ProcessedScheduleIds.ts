export interface ProcessedScheduleIds {
    id: number;
    schedule_id: string;
    processed_at?: Date;
    upload_date_epoch?: number;
    slot_count?: number;
    is_temp?: boolean;
    topic?: string;
}

export class ProcessedScheduleIdsModel {
    static tableName = 'processed_schedule_ids';

    static createTableQuery = `
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

    static async findAll(client: any): Promise<ProcessedScheduleIds[]> {
        const query = `SELECT * FROM ${this.tableName} ORDER BY processed_at DESC`;
        const result = await client.query(query);
        return result.rows;
    }

    static async findById(client: any, id: number): Promise<ProcessedScheduleIds | null> {
        const query = `SELECT * FROM ${this.tableName} WHERE id = $1`;
        const result = await client.query(query, [id]);
        return result.rows[0] || null;
    }

    static async findByScheduleId(client: any, scheduleId: string): Promise<ProcessedScheduleIds | null> {
        const query = `SELECT * FROM ${this.tableName} WHERE schedule_id = $1`;
        const result = await client.query(query, [scheduleId]);
        return result.rows[0] || null;
    }

    static async create(client: any, data: Omit<ProcessedScheduleIds, 'id' | 'processed_at'>): Promise<ProcessedScheduleIds> {
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
        const result = await client.query(query, values);
        return result.rows[0];
    }

    static async update(client: any, id: number, data: Partial<ProcessedScheduleIds>): Promise<ProcessedScheduleIds | null> {
        const fields = Object.keys(data).filter(key => key !== 'id');
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
}
