import { Database } from '../database';
import { PowerUsageModel } from '../models/PowerUsage';

// Sample room data that matches typical classroom/office usage patterns
const ROOMS = [
    'RM301', 'RM302', 'RM303', 'RM304', 'RM305',
    'LAB01', 'LAB02', 'OFFICE01', 'LIBRARY', 'AUDITORIUM'
];

// Realistic power consumption patterns (in watts)
const POWER_PATTERNS: { [key: string]: { base: number; peak: number; low: number } } = {
    'RM301': { base: 450, peak: 850, low: 200 },    // Classroom with AC and projector
    'RM302': { base: 380, peak: 720, low: 180 },    // Smaller classroom
    'RM303': { base: 420, peak: 800, low: 190 },    // Standard classroom
    'RM304': { base: 460, peak: 900, low: 210 },    // Larger classroom
    'RM305': { base: 400, peak: 750, low: 170 },    // Medium classroom
    'LAB01': { base: 1200, peak: 2500, low: 300 },  // Computer lab with high power equipment
    'LAB02': { base: 1100, peak: 2200, low: 280 },  // Science lab with equipment
    'OFFICE01': { base: 280, peak: 450, low: 100 }, // Office space
    'LIBRARY': { base: 600, peak: 900, low: 200 },  // Library with lighting and computers
    'AUDITORIUM': { base: 800, peak: 1800, low: 150 } // Large space with AV equipment
};

function getRealisticPowerUsage(roomId: string, hour: number, dayOfWeek: number): number {
    const pattern = POWER_PATTERNS[roomId] || POWER_PATTERNS['RM301'];
    let powerLevel: number;
    
    // Weekend usage (Saturday = 6, Sunday = 0)
    if (dayOfWeek === 0 || dayOfWeek === 6) {
        // Very low usage on weekends
        powerLevel = pattern.low + Math.random() * (pattern.base - pattern.low) * 0.3;
    }
    // Weekday usage patterns
    else if (hour >= 6 && hour <= 8) {
        // Early morning - building warming up
        powerLevel = pattern.low + Math.random() * (pattern.base - pattern.low) * 0.7;
    }
    else if (hour >= 9 && hour <= 11) {
        // Peak morning hours
        powerLevel = pattern.base + Math.random() * (pattern.peak - pattern.base) * 0.8;
    }
    else if (hour >= 12 && hour <= 13) {
        // Lunch time - reduced usage
        powerLevel = pattern.base + Math.random() * (pattern.peak - pattern.base) * 0.4;
    }
    else if (hour >= 14 && hour <= 16) {
        // Peak afternoon hours
        powerLevel = pattern.base + Math.random() * (pattern.peak - pattern.base) * 0.9;
    }
    else if (hour >= 17 && hour <= 19) {
        // Evening classes
        powerLevel = pattern.base + Math.random() * (pattern.peak - pattern.base) * 0.6;
    }
    else if (hour >= 20 && hour <= 22) {
        // Late evening - cleaning/maintenance
        powerLevel = pattern.low + Math.random() * (pattern.base - pattern.low) * 0.5;
    }
    else {
        // Night time - minimal usage
        powerLevel = pattern.low + Math.random() * 50;
    }
    
    // Add some random variation (±10%)
    const variation = (Math.random() - 0.5) * 0.2;
    powerLevel *= (1 + variation);
    
    // Ensure minimum power (standby consumption)
    return Math.max(Math.round(powerLevel), 50);
}

async function populateTestData() {
    console.log('🔌 Starting power usage data population...');
    
    try {
        await Database.initialize();
        const client = await Database.getClient();
        
        try {
            // Create the power_usage table first
            console.log('Creating power_usage table...');
            await client.query(PowerUsageModel.createTableQuery);
            
            // Clear existing test data (optional)
            console.log('Clearing existing power usage data...');
            await client.query('DELETE FROM power_usage');
            
            let dataCount = 0;
            const batchSize = 1000;
            let batch: any[] = [];
            
            // Generate data for the last 14 days
            const endDate = new Date();
            const startDate = new Date(endDate.getTime() - (14 * 24 * 60 * 60 * 1000));
            
            console.log(`Generating data from ${startDate.toISOString()} to ${endDate.toISOString()}`);
            
            // Generate data every 5 minutes (like the ESP32 code)
            for (let date = new Date(startDate); date <= endDate; date.setMinutes(date.getMinutes() + 5)) {
                const hour = date.getHours();
                const dayOfWeek = date.getDay();
                
                for (const roomId of ROOMS) {
                    const powerWatts = getRealisticPowerUsage(roomId, hour, dayOfWeek);
                    
                    batch.push({
                        timestamp: new Date(date),
                        room_id: roomId,
                        power_watts: powerWatts,
                        device_id: `ESP32_${roomId}`,
                        voltage: 220 + (Math.random() - 0.5) * 10, // 220V ± 5V
                        current: powerWatts / 220, // Calculate current from power
                        frequency: 50 + (Math.random() - 0.5) * 0.5, // 50Hz ± 0.25Hz
                        power_factor: 0.85 + Math.random() * 0.15 // 0.85-1.0 power factor
                    });
                    
                    // Insert batch when it reaches the batch size
                    if (batch.length >= batchSize) {
                        await insertBatch(client, batch);
                        dataCount += batch.length;
                        console.log(`📊 Inserted ${batch.length} records. Total: ${dataCount}`);
                        batch = [];
                    }
                }
            }
            
            // Insert remaining data
            if (batch.length > 0) {
                await insertBatch(client, batch);
                console.log(`📊 Inserted final ${batch.length} records`);
            }
            
            // Get final count
            const countResult = await client.query('SELECT COUNT(*) as total FROM power_usage');
            const totalRecords = countResult.rows[0].total;
            
            console.log(`✅ Successfully populated ${totalRecords} power usage records`);
            
            // Show summary statistics
            const summaryResult = await client.query(`
                SELECT 
                    room_id,
                    COUNT(*) as readings,
                    AVG(power_watts)::numeric(10,2) as avg_watts,
                    MIN(power_watts) as min_watts,
                    MAX(power_watts) as max_watts
                FROM power_usage 
                GROUP BY room_id 
                ORDER BY room_id
            `);
            
            console.log('\n📈 Summary by Room:');
            console.log('Room ID\t\tReadings\tAvg Power\tMin Power\tMax Power');
            console.log('-------\t\t--------\t---------\t---------\t---------');
            summaryResult.rows.forEach(row => {
                console.log(`${row.room_id}\t\t${row.readings}\t\t${row.avg_watts}W\t\t${row.min_watts}W\t\t${row.max_watts}W`);
            });
            
        } finally {
            client.release();
        }
        
    } catch (error) {
        console.error('❌ Error populating test data:', error);
        throw error;
    }
}

async function insertBatch(client: any, batch: any[]) {
    const values: any[] = [];
    let paramCounter = 1;
    
    const valueStrings = batch.map(() => {
        const paramPlaceholders = Array.from({length: 8}, () => `$${paramCounter++}`).join(', ');
        return `(${paramPlaceholders})`;
    }).join(', ');
    
    batch.forEach(record => {
        values.push(
            record.timestamp,
            record.room_id,
            record.power_watts,
            record.device_id,
            record.voltage,
            record.current,
            record.frequency,
            record.power_factor
        );
    });
    
    const query = `
        INSERT INTO power_usage (timestamp, room_id, power_watts, device_id, voltage, current, frequency, power_factor)
        VALUES ${valueStrings}
    `;
    
    await client.query(query, values);
}

// Test MQTT simulation
async function simulateMQTTData() {
    console.log('🎯 Simulating live MQTT data...');
    
    try {
        const client = await Database.getClient();
        
        try {
            // Simulate current readings for all rooms
            for (const roomId of ROOMS) {
                const currentHour = new Date().getHours();
                const currentDay = new Date().getDay();
                const powerWatts = getRealisticPowerUsage(roomId, currentHour, currentDay);
                
                await PowerUsageModel.insertPowerUsage(client, {
                    timestamp: new Date(),
                    room_id: roomId,
                    power_watts: powerWatts,
                    device_id: `ESP32_${roomId}`,
                    voltage: 220 + (Math.random() - 0.5) * 10,
                    current: powerWatts / 220,
                    frequency: 50 + (Math.random() - 0.5) * 0.5,
                    power_factor: 0.85 + Math.random() * 0.15
                });
                
                console.log(`📡 Simulated MQTT data for ${roomId}: ${powerWatts}W`);
            }
            
            console.log('✅ Live data simulation complete');
            
        } finally {
            client.release();
        }
        
    } catch (error) {
        console.error('❌ Error simulating MQTT data:', error);
        throw error;
    }
}

// Main execution
async function main() {
    try {
        console.log('🚀 Power Usage Data Population & Testing');
        console.log('========================================\n');
        
        // Step 1: Populate historical data
        await populateTestData();
        
        // Step 2: Simulate current live data
        await simulateMQTTData();
        
        console.log('\n🎉 All test data populated successfully!');
        console.log('You can now test the power usage dashboard at: http://localhost:8080/power-usage');
        
    } catch (error) {
        console.error('❌ Test execution failed:', error);
        process.exit(1);
    } finally {
        await Database.close();
        process.exit(0);
    }
}

// Run if called directly
if (require.main === module) {
    main();
}

export { populateTestData, simulateMQTTData, getRealisticPowerUsage };