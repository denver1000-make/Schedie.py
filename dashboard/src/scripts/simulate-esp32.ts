import * as mqtt from 'mqtt';
import { getRealisticPowerUsage } from './populate-power-data';

const ROOMS = [
    'RM301', 'RM302', 'RM303', 'RM304', 'RM305',
    'LAB01', 'LAB02', 'OFFICE01', 'LIBRARY', 'AUDITORIUM'
];

// MQTT connection settings
const brokerUrl = 'mqtt://mosquitto_rpi:1883';
const mqttOptions = {
    username: 'denver',
    password: 'denver',
    port: 1883
};

async function simulateESP32Devices() {
    console.log('🎮 Starting ESP32 Power Usage Simulation');
    console.log('=========================================\n');
    
    const client = mqtt.connect(brokerUrl, mqttOptions);
    
    client.on('connect', () => {
        console.log('✅ Connected to MQTT broker');
        console.log('📡 Starting device simulation...\n');
        
        // Simulate each room's ESP32 device
        ROOMS.forEach((roomId, index) => {
            // Stagger the start times to simulate real devices
            setTimeout(() => {
                startDeviceSimulation(client, roomId);
            }, index * 1000); // 1 second delay between each device
        });
    });
    
    client.on('error', (error) => {
        console.error('❌ MQTT connection error:', error);
    });
    
    client.on('close', () => {
        console.log('🔌 MQTT connection closed');
    });
    
    // Keep the simulation running
    process.on('SIGINT', () => {
        console.log('\n⏹️  Stopping simulation...');
        client.end();
        process.exit(0);
    });
}

function startDeviceSimulation(client: mqtt.MqttClient, roomId: string) {
    console.log(`🏢 Starting simulation for ${roomId} (ESP32_${roomId})`);
    
    // Send power readings every 5 seconds (ESP32 sends every 5 seconds in the real code)
    const interval = setInterval(() => {
        const currentHour = new Date().getHours();
        const currentDay = new Date().getDay();
        const powerWatts = getRealisticPowerUsage(roomId, currentHour, currentDay);
        
        const topic = `usage_report/${roomId}`;
        
        client.publish(topic, powerWatts.toString(), (err) => {
            if (err) {
                console.error(`❌ Failed to publish to ${topic}:`, err);
            } else {
                console.log(`📊 ${roomId}: ${powerWatts}W → ${topic}`);
            }
        });
        
    }, 5000); // Every 5 seconds
    
    // Also simulate room shutdown warnings occasionally
    setInterval(() => {
        // 5% chance of sending a shutdown warning
        if (Math.random() < 0.05) {
            const shutdownTopic = `room_shutdown_warning/${roomId}`;
            client.publish(shutdownTopic, roomId, (err) => {
                if (err) {
                    console.error(`❌ Failed to publish shutdown warning to ${shutdownTopic}:`, err);
                } else {
                    console.log(`⚠️  ${roomId}: Shutdown warning sent → ${shutdownTopic}`);
                }
            });
        }
    }, 30000); // Check every 30 seconds
}

// Run the simulation
if (require.main === module) {
    console.log('ESP32 Power Usage MQTT Simulation');
    console.log('This simulates multiple ESP32 devices sending power usage data');
    console.log('Press Ctrl+C to stop the simulation\n');
    
    simulateESP32Devices();
}

export { simulateESP32Devices };