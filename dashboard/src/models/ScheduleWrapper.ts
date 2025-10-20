export interface ScheduleWrapper {
    schedule_id: string;
    upload_date_epoch: number;
    is_temporary: boolean;
    is_synced_to_remote: boolean;
    is_from_remote: boolean;
    in_use: boolean;
}

export class ScheduleWrapperModel {
    static tableName = 'schedule_wrappers_v2';

    static createTableQuery = `
        CREATE TABLE IF NOT EXISTS public.schedule_wrappers_v2 (
            schedule_id character varying NOT NULL,
            upload_date_epoch double precision NOT NULL,
            is_temporary boolean NOT NULL,
            is_synced_to_remote boolean NOT NULL,
            is_from_remote boolean NOT NULL,
            in_use boolean NOT NULL,
            CONSTRAINT schedule_wrappers_v2_pkey PRIMARY KEY (schedule_id)
        );
    `;

    static async findAll(client: any): Promise<ScheduleWrapper[]> {
        const query = `SELECT * FROM ${this.tableName} ORDER BY upload_date_epoch DESC`;
        const result = await client.query(query);
        return result.rows;
    }

    static async findMostRecent(client: any): Promise<ScheduleWrapper | null> {
        const query = `
            SELECT * FROM ${this.tableName} 
            ORDER BY upload_date_epoch DESC 
            LIMIT 1
        `;
        const result = await client.query(query, []);
        return result.rows[0] || null;
    }

    static async findMostRecentPermanent(client: any): Promise<ScheduleWrapper | null> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE is_temporary = false AND in_use = true
            ORDER BY upload_date_epoch DESC 
            LIMIT 1
        `;
        const result = await client.query(query);
        return result.rows[0] || null;
    }

    static async findActiveTemporary(client: any): Promise<ScheduleWrapper[]> {
        const query = `
            SELECT * FROM ${this.tableName} 
            WHERE is_temporary = true 
            ORDER BY upload_date_epoch DESC
        `;
        const result = await client.query(query);
        return result.rows;
    }

    static async findByScheduleId(client: any, scheduleId: string): Promise<ScheduleWrapper | null> {
        const query = `SELECT * FROM ${this.tableName} WHERE schedule_id = $1`;
        const result = await client.query(query, [scheduleId]);
        return result.rows[0] || null;
    }

    static async create(client: any, data: ScheduleWrapper): Promise<ScheduleWrapper> {
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
        const result = await client.query(query, values);
        return result.rows[0];
    }

    static async update(client: any, scheduleId: string, data: Partial<ScheduleWrapper>): Promise<ScheduleWrapper | null> {
        const fields = Object.keys(data).filter(key => key !== 'schedule_id');
        const setClause = fields.map((field, index) => `${field} = $${index + 2}`).join(', ');
        const values = [scheduleId, ...fields.map(field => (data as any)[field])];

        const query = `
            UPDATE ${this.tableName}
            SET ${setClause}
            WHERE schedule_id = $1
            RETURNING *
        `;
        const result = await client.query(query, values);
        return result.rows[0] || null;
    }

    static async delete(client: any, scheduleId: string): Promise<boolean> {
        const query = `DELETE FROM ${this.tableName} WHERE schedule_id = $1`;
        const result = await client.query(query, [scheduleId]);
        return result.rowCount > 0;
    }
}