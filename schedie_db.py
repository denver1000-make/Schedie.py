import sqlite3

import sqlite3


class ScheduleDb:
    def __init__(self, db_path):
        self.connection = sqlite3.connect(database=db_path)
        self.cursor = self.connection.cursor()
        self.setup_table()

    def setup_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ScheduleEnds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_start TEXT NOT NULL,
                schedule_end TEXT NOT NULL,
                room_id TEXT NOT NULL
            )
        ''')
        self.connection.commit()

    def insert_schedule_end(self, schedule_start, schedule_end, room_id):
        self.cursor.execute('INSERT INTO ScheduleEnds (schedule_start, schedule_end, room_id) VALUES (?, ?)',
                            (schedule_start, schedule_end, room_id))
        self.connection.commit()

    def remove_schedule_end(self, schedule_start, schedule_end, room_id):
        self.cursor.execute('DELETE FROM ScheduleEnds WHERE schedule_start=? && schedule_end=? && room_id=?',
                            (schedule_start, schedule_end, room_id))
        self.connection.commit()

    def get_all_ending_schedules(self):
        self.cursor.execute('SELECT * FROM users')
        return self.cursor.fetchall()

    def close(self):
        self.connection.close()
