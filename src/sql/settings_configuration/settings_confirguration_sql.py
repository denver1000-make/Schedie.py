from typing import Optional
from psycopg2 import pool
import psycopg2

from src.modelsV2.models import SettingsConfiguration

# Fixed ID for the single settings configuration entry
SETTINGS_CONFIG_ID = 1


def pg_create_settings_configuration_table(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Create the settings_configuration table for storing system settings
    Uses a fixed ID approach to ensure only one settings entry exists
    
    Args:
        conn_pool: PostgreSQL connection pool
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS settings_configuration (
            id INTEGER PRIMARY KEY DEFAULT 1,
            minute_mark_to_warn INTEGER NOT NULL DEFAULT 5,
            minute_mark_to_skip INTEGER NOT NULL DEFAULT 15,
            bypass_admin_approval BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT single_settings_row CHECK (id = 1)
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create a trigger to automatically update the updated_at timestamp
        cursor.execute("""
        CREATE OR REPLACE FUNCTION update_settings_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql'
        """)
        
        cursor.execute("""
        DROP TRIGGER IF EXISTS update_settings_configuration_updated_at ON settings_configuration
        """)
        
        cursor.execute("""
        CREATE TRIGGER update_settings_configuration_updated_at
        BEFORE UPDATE ON settings_configuration
        FOR EACH ROW EXECUTE FUNCTION update_settings_updated_at()
        """)
        
        # Check if the default row exists, if not create it
        cursor.execute("SELECT COUNT(*) FROM settings_configuration WHERE id = %s", (SETTINGS_CONFIG_ID,))
        row_count = cursor.fetchone()[0]
        
        if row_count == 0:
            # Insert default settings row
            cursor.execute("""
            INSERT INTO settings_configuration (id, minute_mark_to_warn, minute_mark_to_skip, bypass_admin_approval) 
            VALUES (%s, %s, %s, %s)
            """, (SETTINGS_CONFIG_ID, 5, 15, False))  # Default values: warn=5min, skip=15min, bypass=False
            print("✅ Created default settings configuration entry")
        else:
            print("✅ Settings configuration entry already exists")
        
        conn.commit()
        print("✅ Created settings_configuration table with constraints, triggers, and default entry")
        return True
        
    except Exception as e:
        print(f"❌ Error creating settings_configuration table: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_get_settings_configuration(conn_pool: pool.SimpleConnectionPool) -> Optional[SettingsConfiguration]:
    """
    Get the current settings configuration (there should only be one)
    
    Args:
        conn_pool: PostgreSQL connection pool
        
    Returns:
        SettingsConfiguration object if found, None otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        query = """
        SELECT minute_mark_to_warn, minute_mark_to_skip, bypass_admin_approval
        FROM settings_configuration 
        WHERE id = %s
        """
        
        cursor.execute(query, (SETTINGS_CONFIG_ID,))
        result = cursor.fetchone()
        
        if result:
            settings = SettingsConfiguration(
                minute_mark_to_warn=result[0],
                minute_mark_to_skip=result[1],
                bypass_admin_approval=result[2]
            )
            print(f"✅ Retrieved settings configuration: warn={settings.minute_mark_to_warn}min, skip={settings.minute_mark_to_skip}min, bypass={settings.bypass_admin_approval}")
            return settings
        else:
            print("⚠️ No settings configuration found")
            return None
        
    except Exception as e:
        print(f"❌ Error getting settings configuration: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_insert_or_update_settings_configuration(conn_pool: pool.SimpleConnectionPool, settings: SettingsConfiguration) -> bool:
    """
    Insert or update the settings configuration (upsert with fixed ID)
    Since we use a fixed ID, this will always update if exists, insert if not
    
    Args:
        conn_pool: PostgreSQL connection pool
        settings: SettingsConfiguration object to insert/update
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        upsert_query = """
        INSERT INTO settings_configuration (
            id, minute_mark_to_warn, minute_mark_to_skip, bypass_admin_approval
        ) VALUES (
            %s, %s, %s, %s
        )
        ON CONFLICT (id) 
        DO UPDATE SET 
            minute_mark_to_warn = EXCLUDED.minute_mark_to_warn,
            minute_mark_to_skip = EXCLUDED.minute_mark_to_skip,
            bypass_admin_approval = EXCLUDED.bypass_admin_approval,
            updated_at = CURRENT_TIMESTAMP
        """
        
        cursor.execute(upsert_query, (
            SETTINGS_CONFIG_ID,
            settings.minute_mark_to_warn,
            settings.minute_mark_to_skip,
            settings.bypass_admin_approval
        ))
        
        conn.commit()
        print(f"✅ Upserted settings configuration: warn={settings.minute_mark_to_warn}min, skip={settings.minute_mark_to_skip}min, bypass={settings.bypass_admin_approval}")
        return True
        
    except Exception as e:
        print(f"❌ Error upserting settings configuration: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_update_minute_mark_to_warn(conn_pool: pool.SimpleConnectionPool, minute_mark: int) -> bool:
    """
    Update only the minute_mark_to_warn setting
    
    Args:
        conn_pool: PostgreSQL connection pool
        minute_mark: New minute mark to warn value
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        update_query = """
        UPDATE settings_configuration 
        SET minute_mark_to_warn = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        
        cursor.execute(update_query, (minute_mark, SETTINGS_CONFIG_ID))
        
        if cursor.rowcount > 0:
            conn.commit()
            print(f"✅ Updated minute_mark_to_warn to {minute_mark} minutes")
            return True
        else:
            print("⚠️ No settings configuration found to update")
            return False
        
    except Exception as e:
        print(f"❌ Error updating minute_mark_to_warn: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_update_minute_mark_to_skip(conn_pool: pool.SimpleConnectionPool, minute_mark: int) -> bool:
    """
    Update only the minute_mark_to_skip setting
    
    Args:
        conn_pool: PostgreSQL connection pool
        minute_mark: New minute mark to skip value
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        update_query = """
        UPDATE settings_configuration 
        SET minute_mark_to_skip = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        
        cursor.execute(update_query, (minute_mark, SETTINGS_CONFIG_ID))
        
        if cursor.rowcount > 0:
            conn.commit()
            print(f"✅ Updated minute_mark_to_skip to {minute_mark} minutes")
            return True
        else:
            print("⚠️ No settings configuration found to update")
            return False
        
    except Exception as e:
        print(f"❌ Error updating minute_mark_to_skip: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_update_bypass_admin_approval(conn_pool: pool.SimpleConnectionPool, bypass: bool) -> bool:
    """
    Update only the bypass_admin_approval setting
    
    Args:
        conn_pool: PostgreSQL connection pool
        bypass: New bypass admin approval value
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        update_query = """
        UPDATE settings_configuration 
        SET bypass_admin_approval = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        
        cursor.execute(update_query, (bypass, SETTINGS_CONFIG_ID))
        
        if cursor.rowcount > 0:
            conn.commit()
            print(f"✅ Updated bypass_admin_approval to {bypass}")
            return True
        else:
            print("⚠️ No settings configuration found to update")
            return False
        
    except Exception as e:
        print(f"❌ Error updating bypass_admin_approval: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_update_all_settings(conn_pool: pool.SimpleConnectionPool, minute_warn: int, minute_skip: int, bypass: bool) -> bool:
    """
    Update all settings at once
    
    Args:
        conn_pool: PostgreSQL connection pool
        minute_warn: New minute mark to warn value
        minute_skip: New minute mark to skip value
        bypass: New bypass admin approval value
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        update_query = """
        UPDATE settings_configuration 
        SET minute_mark_to_warn = %s,
            minute_mark_to_skip = %s,
            bypass_admin_approval = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """
        
        cursor.execute(update_query, (minute_warn, minute_skip, bypass, SETTINGS_CONFIG_ID))
        
        if cursor.rowcount > 0:
            conn.commit()
            print(f"✅ Updated all settings: warn={minute_warn}min, skip={minute_skip}min, bypass={bypass}")
            return True
        else:
            print("⚠️ No settings configuration found to update")
            return False
        
    except Exception as e:
        print(f"❌ Error updating all settings: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_initialize_default_settings(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Initialize default settings configuration if none exists
    
    Args:
        conn_pool: PostgreSQL connection pool
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        # First check if settings exist
        existing_settings = pg_get_settings_configuration(conn_pool)
        if existing_settings is not None:
            print("⚠️ Settings configuration already exists, skipping initialization")
            return True
        
        # Create default settings
        default_settings = SettingsConfiguration(
            minute_mark_to_warn=5,      # Default: warn 5 minutes before
            minute_mark_to_skip=15,     # Default: skip after 15 minutes
            bypass_admin_approval=False  # Default: require admin approval
        )
        
        success = pg_insert_or_update_settings_configuration(conn_pool, default_settings)
        if success:
            print("✅ Initialized default settings configuration")
        return success
        
    except Exception as e:
        print(f"❌ Error initializing default settings: {e}")
        return False


def pg_clear_settings_configuration(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Clear the settings configuration (delete the single row)
    
    Args:
        conn_pool: PostgreSQL connection pool
        
    Returns:
        bool: True if successful, False otherwise
    """
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        
        # Get count before deletion for reporting
        cursor.execute("SELECT COUNT(*) FROM settings_configuration")
        settings_count = cursor.fetchone()[0]
        
        # Delete the settings configuration
        cursor.execute("DELETE FROM settings_configuration WHERE id = %s", (SETTINGS_CONFIG_ID,))
        
        conn.commit()
        
        print(f"✅ Cleared settings configuration: {settings_count} entry deleted")
        return True
        
    except Exception as e:
        print(f"❌ Error clearing settings configuration: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)


def pg_reset_to_default_settings(conn_pool: pool.SimpleConnectionPool) -> bool:
    """
    Reset settings to default values
    
    Args:
        conn_pool: PostgreSQL connection pool
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Clear existing settings
        clear_success = pg_clear_settings_configuration(conn_pool)
        if not clear_success:
            return False
        
        # Initialize with defaults
        init_success = pg_initialize_default_settings(conn_pool)
        if init_success:
            print("✅ Settings reset to default values")
        
        return init_success
        
    except Exception as e:
        print(f"❌ Error resetting settings to defaults: {e}")
        return False