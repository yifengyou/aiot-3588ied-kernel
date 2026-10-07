package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"

	"github.com/creack/pty"
	"github.com/nwaples/rardecode/v2"
	"golang.org/x/net/websocket"
)

//go:embed index.html
var indexHTML string

//go:embed res
var resFS embed.FS

// buildTime 由 -ldflags "-X main.buildTime=..." 在编译时注入，用于标记版本
var buildTime = "unknown"

// uploadDir 是所有本次上传相关临时文件（压缩包、单文件固件）的根目录
var uploadDir = "/tmp/kdev"

// unpackDir 是压缩包解压后的目标目录，位于 uploadDir 之下
var unpackDir = filepath.Join(uploadDir, "unpack")

const blockSize = 4 * 1024 * 1024

// paramSectorSize 是 parameter.txt 中 mtdparts 分区表 offset/size 的单位（扇区大小，字节）。
// Rockchip parameter.txt 里的数值是以 512 字节扇区为单位的，写入目标设备前需要换算成字节偏移量。
const paramSectorSize = 512

// gptReservedSectors 是磁盘头部预留给保护性 MBR + GPT 主分区表头 + 分区表数组的最小扇区数
// （1 个保护性 MBR + 1 个 GPT 头 + 128 个 128 字节分区表项 = 32 扇区，共 34 扇区）。
// parameter.txt 中起始扇区落在此区域内的分区（通常是 loader/idblock 等极早期区域）不通过
// GPT 管理，仍按原始偏移直接写入。
const gptReservedSectors = 34

// gptBackupSectors 是磁盘尾部预留给备份 GPT（备份分区表数组 32 扇区 + 备份头 1 扇区）的扇区数。
const gptBackupSectors = 33

// ============ Logging (terminal + /tmp/kdev.log) ============

const (
	logFilePath = "/tmp/kdev.log"
	logMaxSize  = 10 * 1024 * 1024 // 日志文件上限 10MB
)

// limitedFileWriter 以追加方式写入日志文件，并保证文件大小不超过 max。
// 当再写入一条日志会超过上限时，先清空文件再继续写入（相当于循环覆盖）。
// Write 永远不返回错误，避免文件写入失败时连带阻断 io.MultiWriter 中的终端输出。
type limitedFileWriter struct {
	mu   sync.Mutex
	path string
	max  int64
	f    *os.File
	size int64
}

func newLimitedFileWriter(path string, max int64) (*limitedFileWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &limitedFileWriter{path: path, max: max, f: f, size: st.Size()}
	// 上次遗留的文件如果已超限，启动时先清空
	if w.size > w.max {
		if err := f.Truncate(0); err != nil {
			f.Close()
			return nil, err
		}
		w.size = 0
	}
	return w, nil
}

func (w *limitedFileWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	total := len(p)
	if int64(len(p)) > w.max {
		p = p[len(p)-int(w.max):] // 单条日志本身超限时只保留尾部
	}

	if w.size+int64(len(p)) > w.max {
		// O_APPEND 模式下 Truncate(0) 后，后续写入自动从 0 偏移开始
		if err := w.f.Truncate(0); err != nil {
			fmt.Fprintf(os.Stderr, "日志文件截断失败: %v\n", err)
			return total, nil
		}
		w.size = 0
	}

	n, err := w.f.Write(p)
	w.size += int64(n)
	if err != nil {
		fmt.Fprintf(os.Stderr, "日志文件写入失败: %v\n", err)
	}
	return total, nil
}

// initLogging 让所有 log 输出同时写到终端(stdout)和 /tmp/kdev.log
func initLogging() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	fw, err := newLimitedFileWriter(logFilePath, logMaxSize)
	if err != nil {
		log.SetOutput(os.Stdout)
		log.Printf("[LOG] 无法打开日志文件 %s，仅输出到终端: %v", logFilePath, err)
		return
	}
	log.SetOutput(io.MultiWriter(os.Stdout, fw))
	log.Printf("[LOG] 日志已启用：终端 + %s（追加写入，上限 %d MB）", logFilePath, logMaxSize/1024/1024)
}

// logRequests 记录 HTTP 请求（跳过高频的进度轮询和静态资源，避免刷屏）。
// 注意：不包装 ResponseWriter，因此不影响 SSE(Flusher) 与 WebSocket(Hijacker)。
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if p != "/api/progress" && p != "/api/upload_status" && p != "/api/logs" && !strings.HasPrefix(p, "/res/") {
			log.Printf("[HTTP] %s %s from %s", r.Method, p, r.RemoteAddr)
			advLog("info", "[HTTP] %s %s from %s", r.Method, p, r.RemoteAddr)
		}
		next.ServeHTTP(w, r)
	})
}

// ============ Advanced Mode Log Hub ============

type LogEntry struct {
	Timestamp string `json:"ts"`
	Level     string `json:"level"`
	Message   string `json:"msg"`
}

type logHub struct {
	mu      sync.RWMutex
	subs    map[chan LogEntry]struct{}
	history []LogEntry
	maxHist int
}

var hub = &logHub{
	subs:    make(map[chan LogEntry]struct{}),
	maxHist: 10000,
}

func (h *logHub) subscribe() chan LogEntry {
	ch := make(chan LogEntry, 256)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *logHub) unsubscribe(ch chan LogEntry) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

func (h *logHub) broadcast(entry LogEntry) {
	h.mu.Lock()
	if len(h.history) >= h.maxHist {
		h.history = h.history[len(h.history)-h.maxHist+1:]
	}
	h.history = append(h.history, entry)
	subs := make([]chan LogEntry, 0, len(h.subs))
	for ch := range h.subs {
		subs = append(subs, ch)
	}
	h.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- entry:
		default:
		}
	}
}

func (h *logHub) getHistory() []LogEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	cp := make([]LogEntry, len(h.history))
	copy(cp, h.history)
	return cp
}

func advLog(level, format string, args ...interface{}) {
	entry := LogEntry{
		Timestamp: time.Now().Format("2006-01-02 15:04:05.000"),
		Level:     level,
		Message:   fmt.Sprintf(format, args...),
	}
	hub.broadcast(entry)
}

func handleLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}

	history := hub.getHistory()
	for _, entry := range history {
		data, _ := json.Marshal(entry)
		fmt.Fprintf(w, "data: %s\n\n", data)
	}
	flusher.Flush()

	ch := hub.subscribe()
	defer hub.unsubscribe(ch)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-ch:
			data, _ := json.Marshal(entry)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// ============ Upload Progress ============

// uploadState 记录当前这次上传的实时状态，供前端通过 /api/upload_status 轮询。
// Received 统计的是从网络实际收到的请求体字节数（含 multipart 头，略大于文件本身），
// Total 为请求 Content-Length，因此进度百分比是近似值。
// Stage: uploading=接收并写入磁盘；streaming=边接收边解压(tar.gz/tar.bz2)；extracting=接收完毕后解压(zip/rar)
type uploadState struct {
	Active       bool    `json:"active"`
	Stage        string  `json:"stage"`
	Name         string  `json:"name"`
	Total        int64   `json:"total"`
	Received     int64   `json:"received"`
	Progress     float64 `json:"progress"`
	Speed        float64 `json:"speed"`        // 字节/秒
	ExtractElapsed float64 `json:"extract_elapsed"` // 解压已用时间（秒）
}

var (
	upMu        sync.Mutex
	upState     uploadState
	upStart     time.Time
	upExtractStart time.Time
)

func upBegin(total int64) {
	upMu.Lock()
	upState = uploadState{Active: true, Stage: "uploading", Total: total}
	upStart = time.Now()
	upMu.Unlock()
}

func upSet(name, stage string) {
	upMu.Lock()
	upState.Name = name
	upState.Stage = stage
	upMu.Unlock()
}

func upSetStage(stage string) {
	upMu.Lock()
	upState.Stage = stage
	if stage == "extracting" {
		upExtractStart = time.Now()
	}
	if upExtractStart.IsZero() == false && stage != "extracting" {
		upState.ExtractElapsed = 0
	}
	upMu.Unlock()
}

func upEnd() {
	upMu.Lock()
	upState.Active = false
	st, el := upState, time.Since(upStart).Seconds()
	upMu.Unlock()
	if el > 0 {
		log.Printf("[UPLOAD] 传输结束: %s 共收到 %d bytes，用时 %.1fs，平均 %.2f MB/s",
			st.Name, st.Received, el, float64(st.Received)/el/1024/1024)
	}
}

// countingBody 包装请求体，统计实际收到的字节数（不改变读取行为）
type countingBody struct{ io.ReadCloser }

func (c *countingBody) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if n > 0 {
		upMu.Lock()
		upState.Received += int64(n)
		if el := time.Since(upStart).Seconds(); el > 0 {
			upState.Speed = float64(upState.Received) / el
		}
		if upState.Total > 0 {
			upState.Progress = float64(upState.Received) / float64(upState.Total) * 100
			if upState.Progress > 100 {
				upState.Progress = 100
			}
		}
		upMu.Unlock()
	}
	return n, err
}

func handleUploadStatus(w http.ResponseWriter, r *http.Request) {
	upMu.Lock()
	st := upState
	if st.Stage == "extracting" && !upExtractStart.IsZero() {
		st.ExtractElapsed = time.Since(upExtractStart).Seconds()
	}
	upMu.Unlock()
	w.Header().Set("Cache-Control", "no-store")
	jsonOK(w, st)
}

// ============ JSON Helpers ============

func jsonOK(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ============ Flash Task Management ============

type FlashWriteItem struct {
	Name   string
	Offset int64
	Size   int64
	Path   string
}

type FlashTask struct {
	ID       string  `json:"id"`
	Status   string  `json:"status"`
	Progress float64 `json:"progress"`
	Written  int64   `json:"written"`
	Total    int64   `json:"total"`
	Speed    float64 `json:"speed"`
	Error    string  `json:"error,omitempty"`
	Mode     string  `json:"mode,omitempty"`

	Items        []FlashWriteItem `json:"-"`
	Partitions   []ParamPartition `json:"-"` // 仅 Mode=="multi" 时有效，用于刷写前重建 GPT 分区表
	TargetDev    string           `json:"-"`
}

var (
	tasks   = make(map[string]*FlashTask)
	tasksMu sync.RWMutex
	taskSeq int
)

func newTask(target string, total int64) *FlashTask {
	tasksMu.Lock()
	defer tasksMu.Unlock()
	taskSeq++
	id := fmt.Sprintf("task_%d_%d", time.Now().UnixNano(), taskSeq)
	t := &FlashTask{ID: id, Status: "writing", Total: total, TargetDev: target}
	tasks[id] = t
	return t
}

func getTask(id string) *FlashTask {
	tasksMu.RLock()
	defer tasksMu.RUnlock()
	return tasks[id]
}

func updateTask(id string, fn func(*FlashTask)) {
	tasksMu.Lock()
	defer tasksMu.Unlock()
	if t, ok := tasks[id]; ok {
		fn(t)
	}
}

// ============ Block Device Structures ============

type BlockDevice struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	Type  string `json:"type"`
	Size  string `json:"size"`
	Model string `json:"model"`
}

type LsblkOutput struct {
	BlockDevices []LsblkDevice `json:"blockdevices"`
}

type LsblkDevice struct {
	Name  string      `json:"name"`
	Size  json.Number `json:"size"`
	Type  string      `json:"type"`
	Model *string     `json:"model"`
	Tran  *string     `json:"tran"`
}

// ============ Main ============

func main() {
	initLogging()

	tmpl := template.Must(template.New("index").Parse(indexHTML))

	resSubFS, err := fs.Sub(resFS, "res")
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		tmpl.Execute(w, map[string]string{"BuildTime": buildTime})
	})
	http.HandleFunc("/api/devices", func(w http.ResponseWriter, r *http.Request) {
		jsonOK(w, getFilteredDevices())
	})
	http.HandleFunc("/api/reboot", handleReboot)
	http.HandleFunc("/upload", handleUpload)
	http.HandleFunc("/api/progress", handleProgress)
	http.HandleFunc("/api/upload_status", handleUploadStatus)
	http.HandleFunc("/api/logs", handleLogs)
	http.HandleFunc("/api/boot_order", handleBootOrder)
	http.HandleFunc("/api/uboot_setenv", handleUbootSetenv)
	http.HandleFunc("/api/uboot_reset", handleUbootReset)
 	http.HandleFunc("/api/storage", handleStorage)
 	http.HandleFunc("/api/cpu", handleCpu)
 	http.HandleFunc("/api/network", handleNetwork)
 	http.HandleFunc("/api/update_check", handleUpdateCheck)
 	http.HandleFunc("/api/update_download", handleUpdateDownload)
	http.HandleFunc("/api/update_cancel", handleUpdateCancel)
 	http.HandleFunc("/api/update_upload", handleUpdateUpload)
	http.HandleFunc("/api/backup/disks", handleBackupDisks)
	http.HandleFunc("/api/backup/stream", handleBackupStream)
	http.HandleFunc("/api/backup/progress", handleBackupProgress)
	http.HandleFunc("/api/backup/cancel", handleBackupCancel)
	http.HandleFunc("/api/restore/stream", handleRestoreStream)
	http.HandleFunc("/api/firewall/status", handleFirewallStatus)
	http.HandleFunc("/api/firewall/apply", handleFirewallApply)
	http.HandleFunc("/api/firewall/delete", handleFirewallDelete)
	http.HandleFunc("/api/firewall/toggle", handleFirewallToggle)

	http.Handle("/api/terminal", websocket.Handler(handleTerminal))
	http.Handle("/res/", http.StripPrefix("/res/", http.FileServer(http.FS(resSubFS))))

	// 启动横幅使用 log，这样也会写入日志文件
	log.Println("========================================")
	log.Println("  RKdev + Terminal port:80 protocol:http")
	log.Println("  HTTPS(443) -> HTTP(80) redirect")
	log.Println("========================================")

	go startHttpsRedirect()

	log.Fatal(http.ListenAndServe(":80", logRequests(http.DefaultServeMux)))
}

func startHttpsRedirect() {
	cert, err := generateSelfSignedCert()
	if err != nil {
		log.Printf("[REDIRECT] 生成证书失败，443重定向不可用: %v", err)
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if i := strings.Index(host, ":"); i >= 0 {
			host = host[:i]
		}
		if host == "" {
			host = r.RemoteAddr
			if i := strings.LastIndex(host, ":"); i >= 0 {
				host = host[:i]
			}
		}
		http.Redirect(w, r, "http://"+host+"/", http.StatusMovedPermanently)
	})

	srv := &http.Server{
		Addr:      ":443",
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
	}
	log.Fatal(srv.ListenAndServeTLS("", ""))
}

func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"RKdev"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("0.0.0.0")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
	}, nil
}

// ============ Terminal (PTY over WebSocket) ============

func handleTerminal(ws *websocket.Conn) {
	defer ws.Close()
	log.Printf("[TERM] Session started from %s", ws.Request().RemoteAddr)

	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}

	cmd := exec.Command(shell)
	cmd.Env = append(os.Environ(), "TERM=xterm-256color")

	ptmx, err := pty.Start(cmd)
	if err != nil {
		log.Printf("[TERM] PTY start failed: %v", err)
		ws.Write([]byte("\r\n*** Failed to start terminal: " + err.Error() + " ***\r\n"))
		return
	}
	defer ptmx.Close()

	pty.Setsize(ptmx, &pty.Winsize{Rows: 30, Cols: 120})

	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 4096)
		for {
			n, err := ws.Read(buf)
			if err != nil {
				return
			}
			if n > 0 && buf[0] == '0' {
				ptmx.Write(buf[1:n])
			} else if n >= 5 && buf[0] == '1' {
				rows := uint16(buf[1])<<8 | uint16(buf[2])
				cols := uint16(buf[3])<<8 | uint16(buf[4])
				pty.Setsize(ptmx, &pty.Winsize{Rows: rows, Cols: cols})
			}
		}
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 4096)
		for {
			n, err := ptmx.Read(buf)
			if err != nil {
				return
			}
			if _, werr := ws.Write(buf[:n]); werr != nil {
				return
			}
		}
	}()

	<-done
	cmd.Process.Kill()
	cmd.Wait()
	log.Println("[TERM] Session ended")
}

// ============ Reboot ============

func handleReboot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	log.Println("[REBOOT] Requested")
	jsonOK(w, map[string]string{"message": "系统即将重启..."})
	go func() {
		time.Sleep(500 * time.Millisecond)
		exec.Command("/sbin/reboot").Run()
	}()
}

// handleBootOrder 根据 HTTP 方法分发到 GET/POST 处理函数
func handleBootOrder(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleUbootGet(w, r)
	case http.MethodPost:
		handleUbootSet(w, r)
	default:
		jsonErr(w, http.StatusMethodNotAllowed, "仅支持 GET/POST")
	}
}

// ============ U-Boot Environment (fw_printenv / fw_setenv) ============

const (
	mtdblockPath = "/dev/mmcblk0"
	fwEnvConfig  = "/etc/fw_env.config"
	fwEnvContent = "/dev/mmcblk0 0x500000 0x10000 0x10000"
)

var ubootDefaultVars = map[string]string{
	"arch":               "arm",
	"baudrate":           "1500000",
	"boot_one_dev":       "run try_extlinux_boot; run try_bootscr_boot; run try_rockchip_fw; ",
	"boot_targets":       "usb nvme scsi",
	"bootcmd":            "run bootcmd_usb; run bootcmd_emmc;  run bootcmd_nvme; run bootcmd_scsi; echo ERROR: No bootable device found! Enter loader mode; rockusb 0 mtd 2; ",
	"bootcmd_emmc":       "echo EMMC: scanning; setenv devtype mmc; mmc rescan; mmc info; setenv devnum 0; if mmc dev 0; then run boot_one_dev; fi; setenv devnum 1; if mmc dev 1; then run boot_one_dev; fi; setenv devnum 2; if mmc dev 2; then run boot_one_dev; fi; echo EMMC: no emmc bootable media; ",
	"bootcmd_nvme":       "echo NVMe: pci enum; pci enum; nvme scan; setenv devtype nvme; setenv devnum 0; if nvme dev 0; then run boot_one_dev; fi; setenv devnum 1; if nvme dev 1; then run boot_one_dev; fi; echo NVMe: no nvme bootable media; ",
	"bootcmd_recovery":   "sf probe 0;sf read 0x40000000 0x0 0x2000000;blkmap create spidisk;blkmap map spidisk 0 0x10000 mem 0x40000000;part list blkmap 0;sysboot blkmap 0:2 any ${scriptaddr} /recovery.conf;",
	"bootcmd_scsi":       "echo SCSI: scsi scan; scsi scan; setenv devtype scsi; setenv devnum 0; if scsi dev 0; then run boot_one_dev; fi; setenv devnum 1; if scsi dev 1; then run boot_one_dev; fi; echo SCSI: no scsi bootable media; ",
	"bootcmd_usb":        "echo USB: start; usb start; usb info; setenv devtype usb; setenv devnum 0; if usb dev 0; then run boot_one_dev; fi; setenv devnum 1; if usb dev 1; then run boot_one_dev; fi; echo USB: no usb bootable media; ",
	"bootdelay":          "2",
	"button_cmd_0":       "run bootcmd_recovery",
	"button_cmd_0_name":  "Recovery key",
	"cpu":                "armv8",
	"fdt_addr_r":        "0x12000000",
	"fdtfile":            "rockchip/rk3588-aiot-3588ied.dtb",
	"fdtoverlay_addr_r":  "0x12100000",
	"kernel_addr_r":      "0x02000000",
	"kernel_comp_addr_r": "0x0a000000",
	"kernel_comp_size":   "0x8000000",
	"loadaddr":           "0xc00800",
	"pxefile_addr_r":     "0x00e00000",
	"ramdisk_addr_r":     "0x12180000",
	"script_offset_f":    "0xffe000",
	"script_size_f":      "0x2000",
	"scriptaddr":         "0x00c00000",
	"soc":                "rk3588",
	"try_bootscr_boot":   "for distro_bootpart in 1 2 3 4 8 5 6 7 9; do for prefix in / /boot/; do echo Try ${devtype} ${devnum}:${distro_bootpart} ${prefix}boot.scr; if test -e ${devtype} ${devnum}:${distro_bootpart} ${prefix}boot.scr; then echo Found boot.scr on ${devtype} ${devnum}:${distro_bootpart}; load ${devtype} ${devnum}:${distro_bootpart} ${scriptaddr} ${prefix}boot.scr; source ${scriptaddr}; echo boot.scr returned, trying next...; fi; done; done; ",
	"try_extlinux_boot":  "for distro_bootpart in 1 2 3 4 8 5 6 7 9; do for extlinux_path in /boot/extlinux/extlinux.conf /extlinux/extlinux.conf /extlinux.conf; do echo Try ${devtype} ${devnum}:${distro_bootpart} ${extlinux_path}; if test -e ${devtype} ${devnum}:${distro_bootpart} ${extlinux_path}; then echo Found extlinux.conf on ${devtype} ${devnum}:${distro_bootpart}; sysboot ${devtype} ${devnum}:${distro_bootpart} any ${scriptaddr} ${extlinux_path}; echo sysboot returned, trying next...; fi; done; done; ",
	"try_recovery_boot":  "echo Recovery: scanning ${devtype} ${devnum}; if test -e ${devtype} ${devnum}:1 /recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:1; sysboot ${devtype} ${devnum}:1 any ${scriptaddr} /recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:1 /boot/recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:1; sysboot ${devtype} ${devnum}:1 any ${scriptaddr} /boot/recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:1 /recovery/recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:1; sysboot ${devtype} ${devnum}:1 any ${scriptaddr} /recovery/recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:2 /recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:2; sysboot ${devtype} ${devnum}:2 any ${scriptaddr} /recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:2 /boot/recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:2; sysboot ${devtype} ${devnum}:2 any ${scriptaddr} /boot/recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:2 /recovery/recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:2; sysboot ${devtype} ${devnum}:2 any ${scriptaddr} /recovery/recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:3 /recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:3; sysboot ${devtype} ${devnum}:3 any ${scriptaddr} /recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:3 /boot/recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:3; sysboot ${devtype} ${devnum}:3 any ${scriptaddr} /boot/recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:3 /recovery/recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:3; sysboot ${devtype} ${devnum}:3 any ${scriptaddr} /recovery/recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:4 /recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:4; sysboot ${devtype} ${devnum}:4 any ${scriptaddr} /recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:4 /boot/recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:4; sysboot ${devtype} ${devnum}:4 any ${scriptaddr} /boot/recovery.conf; echo sysboot returned, trying next...; fi; if test -e ${devtype} ${devnum}:4 /recovery/recovery.conf; then echo Found recovery.conf on ${devtype} ${devnum}:4; sysboot ${devtype} ${devnum}:4 any ${scriptaddr} /recovery/recovery.conf; echo sysboot returned, trying next...; fi; echo Recovery scan complete, no valid recovery.conf found; ",
	"try_rockchip_fw":    "mw.l 0x01fffff8 0 1; mw.l 0x04fffff8 0 1; mw.l 0x07000000 0 1; read ${devtype} ${devnum}:5 0x01fffff8 0 0x14000; if itest.l *0x01fffff8 == 0x4c4e524b; then echo RKFW: KRNL kernel found on ${devtype} ${devnum}:5; read ${devtype} ${devnum}:6 0x04fffff8 0 0x10000; read ${devtype} ${devnum}:4 0x07000000 0 0x1000; if itest.l *0x04fffff8 == 0x4c4e524b && itest.l *0x07000000 == 0x45435352 && itest.l *0x07000800 == 0xedfe0dd0; then echo RKFW: booting rockchip firmware from ${devtype} ${devnum}; if part uuid ${devtype} ${devnum}:8 rkfw_uuid; then setenv rkfw_root root=PARTUUID=${rkfw_uuid}; elif test ${devtype} = nvme; then setenv rkfw_root root=/dev/nvme0n1p8; else setenv rkfw_root root=/dev/mmcblk${devnum}p8; fi; setenv bootargs ${rkfw_root} rootfstype=ext4 rootwait rw console=ttyS2,1500000n8 earlycon=uart8250,mmio32,0xfeb50000; booti 0x02000000 0x05000000:0x2000000 0x07000800; fi; fi; ",
}

// UbootVar 表示一个 U-Boot 环境变量
type UbootVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// UbootResponse 是获取 U-Boot 环境变量的响应
type UbootResponse struct {
	Vars    []UbootVar `json:"vars"`
	Bootcmd string     `json:"bootcmd"`  // 当前 bootcmd 值
	Order   []string   `json:"order"`    // 解析后的引导顺序（如 ["usb","nvme","sata"]）
	Count   int        `json:"count"`
}

// bootCmdTokens 是 bootcmd 中支持的引导命令及显示名
var bootCmdTokens = []struct {
	Cmd   string
	Label string
	Type  string
}{
	{"bootcmd_usb", "USB", "usb"},
	{"bootcmd_nvme", "NVMe", "nvme"},
	{"bootcmd_scsi", "SATA/SCSI", "sata"},
	{"bootcmd_emmc", "eMMC", "emmc"},
	{"bootcmd_recovery", "Recovery", "recovery"},
	{"bootcmd_maskrom", "Maskrom", "maskrom"},
}

// ensureFwEnvConfig 检测 /dev/mtdblock0 是否存在，若存在则检测 /etc/fw_env.config，
// 不存在则写入默认配置
func ensureFwEnvConfig() error {
	if _, err := os.Stat(mtdblockPath); err != nil {
		return fmt.Errorf("未检测到 %s，无法访问 U-Boot 环境变量", mtdblockPath)
	}

	if _, err := os.Stat(fwEnvConfig); err != nil {
		advLog("warn", "[UBOOT] %s 不存在，写入默认配置: %s", fwEnvConfig, fwEnvContent)
		if err := os.WriteFile(fwEnvConfig, []byte(fwEnvContent+"\n"), 0644); err != nil {
			return fmt.Errorf("写入 %s 失败: %v", fwEnvConfig, err)
		}
		advLog("success", "[UBOOT] %s 已写入", fwEnvConfig)
	}
	return nil
}

// runFwPrintenv 调用 fw_printenv 获取所有 U-Boot 环境变量
func runFwPrintenv() ([]UbootVar, bool, error) {
	out, err := exec.Command("fw_printenv").CombinedOutput()
	badCRC := strings.Contains(string(out), "Bad CRC")
	if err != nil && !badCRC {
		return nil, false, fmt.Errorf("fw_printenv 执行失败: %v, output: %s", err, string(out))
	}

	var vars []UbootVar
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Warning:") {
			continue
		}
		idx := strings.Index(line, "=")
		if idx < 0 {
			continue
		}
		vars = append(vars, UbootVar{
			Name:  line[:idx],
			Value: line[idx+1:],
		})
	}
	return vars, badCRC, nil
}

// handleUbootGet 处理 GET /api/boot_order
// 1. 检测 /dev/mtdblock0 是否存在
// 2. 检测 /etc/fw_env.config 是否存在，不存在则写入
// 3. 调用 fw_printenv 获取所有环境变量
// 4. 解析 bootcmd 提取引导顺序
func resetUbootDefaults() error {
	if err := ensureFwEnvConfig(); err != nil {
		return err
	}

	vars, _, err := runFwPrintenv()
	if err == nil {
		for _, v := range vars {
			if _, ok := ubootDefaultVars[v.Name]; !ok {
				out, delErr := exec.Command("fw_setenv", v.Name).CombinedOutput()
				if delErr != nil {
					advLog("warn", "[UBOOT] 删除非出厂变量 %s 失败: %v, output: %s", v.Name, delErr, string(out))
				} else {
					advLog("info", "[UBOOT] 删除非出厂变量: %s", v.Name)
				}
			}
		}
	}

	for name, value := range ubootDefaultVars {
		out, err := exec.Command("fw_setenv", name, value).CombinedOutput()
		if err != nil {
			return fmt.Errorf("fw_setenv %s 失败: %v, output: %s", name, err, string(out))
		}
		advLog("info", "[UBOOT] 重置变量: %s=%s", name, value)
	}
	return nil
}

func handleUbootGet(w http.ResponseWriter, r *http.Request) {
	if err := ensureFwEnvConfig(); err != nil {
		advLog("error", "[UBOOT] %v", err)
		jsonErr(w, http.StatusNotFound, err.Error())
		return
	}

	vars, badCRC, err := runFwPrintenv()
	if err != nil {
		advLog("error", "[UBOOT] %v", err)
		jsonErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	if badCRC {
		advLog("warn", "[UBOOT] 检测到 Bad CRC，环境变量已损坏，正在自动还原出厂变量...")
		if resetErr := resetUbootDefaults(); resetErr != nil {
			advLog("error", "[UBOOT] 自动还原失败: %v", resetErr)
			jsonErr(w, http.StatusInternalServerError, "环境变量 CRC 校验失败，自动还原出厂变量失败: "+resetErr.Error())
			return
		}
		advLog("success", "[UBOOT] 环境变量已自动还原为出厂值")
		vars, _, err = runFwPrintenv()
		if err != nil {
			advLog("error", "[UBOOT] 重置后重新读取失败: %v", err)
			jsonErr(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	bootcmd := ""
	for _, v := range vars {
		if v.Name == "bootcmd" {
			bootcmd = v.Value
			break
		}
	}

	order := parseBootcmdOrder(bootcmd)

	advLog("info", "[UBOOT] 获取环境变量成功: %d 个变量, bootcmd=%s, 顺序=%v", len(vars), bootcmd, order)
	for _, v := range vars {
		advLog("data", "[UBOOT] %s=%s", v.Name, v.Value)
	}

	jsonOK(w, UbootResponse{
		Vars:    vars,
		Bootcmd: bootcmd,
		Order:   order,
		Count:   len(vars),
	})
}

// handleUbootSet 处理 POST /api/boot_order
// 请求体 JSON: {"bootcmd": "run bootcmd_usb; run bootcmd_nvme; ..."}
// 调用 fw_setenv bootcmd <value> 写入
func handleUbootSet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Bootcmd string `json:"bootcmd"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, http.StatusBadRequest, "解析请求失败: "+err.Error())
		return
	}

	if req.Bootcmd == "" {
		jsonErr(w, http.StatusBadRequest, "bootcmd 不能为空")
		return
	}

	if err := ensureFwEnvConfig(); err != nil {
		advLog("error", "[UBOOT] %v", err)
		jsonErr(w, http.StatusNotFound, err.Error())
		return
	}

	advLog("warn", "[UBOOT] 准备写入 bootcmd=%s", req.Bootcmd)

	out, err := exec.Command("fw_setenv", "bootcmd", req.Bootcmd).CombinedOutput()
	if err != nil {
		advLog("error", "[UBOOT] fw_setenv 失败: %v, output: %s", err, string(out))
		jsonErr(w, http.StatusInternalServerError, fmt.Sprintf("fw_setenv 失败: %v, output: %s", err, string(out)))
		return
	}

	advLog("success", "[UBOOT] bootcmd 已更新: %s", req.Bootcmd)
	log.Printf("[UBOOT] bootcmd 已更新: %s", req.Bootcmd)
	jsonOK(w, map[string]string{"message": "引导顺序已更新，重启后生效"})
}

// parseBootcmdOrder 从 bootcmd 字符串中解析出引导顺序
// bootcmd 格式: run bootcmd_usb; run bootcmd_emmc; run bootcmd_nvme; run bootcmd_scsi;
// 按 bootcmd 中实际出现顺序返回引导类型列表
func parseBootcmdOrder(bootcmd string) []string {
	cmdToType := make(map[string]string)
	for _, t := range bootCmdTokens {
		cmdToType[t.Cmd] = t.Type
	}
	var order []string
	for _, segment := range strings.Split(bootcmd, ";") {
		segment = strings.TrimSpace(segment)
		for cmd, typ := range cmdToType {
			if strings.Contains(segment, cmd) {
				order = append(order, typ)
				break
			}
		}
	}
	return order
}

// buildBootcmd 根据引导类型顺序列表构建 bootcmd 字符串
// 例如 ["usb", "nvme", "sata"] -> "run bootcmd_usb; run bootcmd_nvme; run bootcmd_scsi;"
func buildBootcmd(order []string) string {
	var parts []string
	typeMap := make(map[string]string)
	for _, t := range bootCmdTokens {
		typeMap[t.Type] = t.Cmd
	}
	for _, t := range order {
		if cmd, ok := typeMap[t]; ok {
			parts = append(parts, "run "+cmd)
		}
	}
	return strings.Join(parts, "; ") + ";"
}

// handleUbootSetenv 处理 POST /api/uboot_setenv
// 请求体 JSON: {"name": "变量名", "value": "变量值"}
// 调用 fw_setenv <name> <value> 写入单个 U-Boot 环境变量
func handleUbootSetenv(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}

	var req struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, http.StatusBadRequest, "解析请求失败: "+err.Error())
		return
	}

	if req.Name == "" {
		jsonErr(w, http.StatusBadRequest, "变量名不能为空")
		return
	}

	if err := ensureFwEnvConfig(); err != nil {
		advLog("error", "[UBOOT] %v", err)
		jsonErr(w, http.StatusNotFound, err.Error())
		return
	}

	advLog("warn", "[UBOOT] 准备写入环境变量: %s=%s", req.Name, req.Value)

	out, err := exec.Command("fw_setenv", req.Name, req.Value).CombinedOutput()
	if err != nil {
		advLog("error", "[UBOOT] fw_setenv %s 失败: %v, output: %s", req.Name, err, string(out))
		jsonErr(w, http.StatusInternalServerError, fmt.Sprintf("fw_setenv %s 失败: %v, output: %s", req.Name, err, string(out)))
		return
	}

	advLog("success", "[UBOOT] 环境变量已写入: %s=%s", req.Name, req.Value)
	log.Printf("[UBOOT] 环境变量已写入: %s=%s", req.Name, req.Value)
	jsonOK(w, map[string]string{"message": "环境变量 " + req.Name + " 已保存，重启后生效"})
}

func handleUbootReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}

	if err := ensureFwEnvConfig(); err != nil {
		advLog("error", "[UBOOT] %v", err)
		jsonErr(w, http.StatusNotFound, err.Error())
		return
	}

	advLog("warn", "[UBOOT] 手动还原出厂变量...")
	if err := resetUbootDefaults(); err != nil {
		advLog("error", "[UBOOT] 手动还原失败: %v", err)
		jsonErr(w, http.StatusInternalServerError, "还原出厂变量失败: "+err.Error())
		return
	}

	advLog("success", "[UBOOT] 环境变量已还原为出厂值")
	jsonOK(w, map[string]string{"message": "环境变量已还原为出厂值，重启后生效"})
}

// ============ Upload & Flash ============

// checkFirmwareName 校验单一固件文件名（不含打包格式）
func checkFirmwareName(name string) (string, error) {
	lower := strings.ToLower(name)
	inner := lower
	if strings.HasSuffix(inner, ".gz") {
		inner = inner[:len(inner)-len(".gz")]
	}
	if strings.HasSuffix(inner, ".img") || strings.HasSuffix(inner, ".bin") {
		return name[:len(inner)], nil
	}
	return "", fmt.Errorf("不支持的文件类型 %q，仅支持 .img/.bin 固件及其 gzip 压缩包", name)
}

// ============ Archive Kind Detection ============

// archiveKind 标识上传文件所使用的打包格式
type archiveKind int

const (
	archiveNone archiveKind = iota
	archiveZip
	archiveTarGz
	archiveTarBz2
	archiveRar
)

func (k archiveKind) String() string {
	switch k {
	case archiveZip:
		return "zip"
	case archiveTarGz:
		return "tar.gz"
	case archiveTarBz2:
		return "tar.bz2"
	case archiveRar:
		return "rar"
	default:
		return "none"
	}
}

// detectArchiveKind 根据文件名判断打包格式：支持 zip / tar.gz(.tgz) / tar.bz2(.tbz2/.tbz) / rar
func detectArchiveKind(lowerName string) archiveKind {
	switch {
	case strings.HasSuffix(lowerName, ".zip"):
		return archiveZip
	case strings.HasSuffix(lowerName, ".tar.gz"), strings.HasSuffix(lowerName, ".tgz"):
		return archiveTarGz
	case strings.HasSuffix(lowerName, ".tar.bz2"), strings.HasSuffix(lowerName, ".tbz2"), strings.HasSuffix(lowerName, ".tbz"):
		return archiveTarBz2
	case strings.HasSuffix(lowerName, ".rar"):
		return archiveRar
	}
	return archiveNone
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	log.Printf("[UPLOAD] 收到上传请求 from %s", r.RemoteAddr)
	advLog("info", "[UPLOAD] 收到上传请求, Content-Length=%s, 来源=%s", r.Header.Get("Content-Length"), r.RemoteAddr)
	upBegin(r.ContentLength)
	defer upEnd()
	r.Body = &countingBody{ReadCloser: r.Body}

	if err := os.RemoveAll(uploadDir); err != nil {
		advLog("error", "[UPLOAD] 清理上传目录失败: %v", err)
		jsonErr(w, http.StatusInternalServerError, "清理上传目录失败: "+err.Error())
		return
	}
	advLog("data", "[UPLOAD] 已清理上传目录 %s", uploadDir)
	if err := os.MkdirAll(unpackDir, 0755); err != nil {
		advLog("error", "[UPLOAD] 创建上传目录失败: %v", err)
		jsonErr(w, http.StatusInternalServerError, "创建上传目录失败: "+err.Error())
		return
	}
	advLog("data", "[UPLOAD] 已创建解压目录 %s", unpackDir)

	mr, err := r.MultipartReader()
	if err != nil {
		jsonErr(w, http.StatusBadRequest, "解析表单失败: "+err.Error())
		return
	}

	var target, safeName, innerName, tmpPath string
	var written int64
	var isGzip bool
	var kind archiveKind
	var archivePath string // zip/rar 需要先落盘的路径

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			jsonErr(w, http.StatusBadRequest, "读取表单失败: "+err.Error())
			return
		}

		switch part.FormName() {
		case "target_device":
			b, _ := io.ReadAll(io.LimitReader(part, 1024))
			target = strings.TrimSpace(string(b))

		case "firmware":
			safeName = filepath.Base(part.FileName())
			if safeName == "" || safeName == "." {
				jsonErr(w, http.StatusBadRequest, "获取文件失败: 文件名为空")
				return
			}
			lowerName := strings.ToLower(safeName)
			kind = detectArchiveKind(lowerName)
			advLog("info", "[UPLOAD] 固件文件: %s, 打包格式: %s", safeName, kind)
			stage := "uploading"
			if kind == archiveTarGz || kind == archiveTarBz2 {
				stage = "streaming"
			}
			upSet(safeName, stage)
			advLog("data", "[UPLOAD] 上传阶段: %s", stage)

			// ==== 情况1: tar.gz / tgz — 流式解压，不落盘压缩包 ====
			if kind == archiveTarGz {
				extractDir := filepath.Join(unpackDir, fmt.Sprintf("pkg_%d_targz", time.Now().UnixNano()))
				if err := os.MkdirAll(extractDir, 0755); err != nil {
					jsonErr(w, http.StatusInternalServerError, "创建解压目录失败: "+err.Error())
					return
				}
				advLog("info", "[UPLOAD] 开始流式解压 tar.gz -> %s", extractDir)
				files, err := extractTarGz(part, extractDir)
				if err != nil {
					os.RemoveAll(extractDir)
					advLog("error", "[UPLOAD] 解压 tar.gz 失败: %v", err)
					jsonErr(w, http.StatusBadRequest, "解压 tar.gz 失败: "+err.Error())
					return
				}
				if len(files) == 0 {
					os.RemoveAll(extractDir)
					advLog("error", "[UPLOAD] tar.gz 包内没有常规文件")
					jsonErr(w, http.StatusBadRequest, "tar.gz 包内没有常规文件")
					return
				}
				advLog("success", "[UPLOAD] tar.gz 解压完成, 共 %d 个文件", len(files))
				for _, f := range files {
					if st, e := os.Stat(f); e == nil {
						advLog("data", "[UPLOAD] 解压文件: %s (%s)", filepath.Base(f), formatBytes(st.Size()))
					}
				}
				task, msg, err := buildTaskFromExtracted(files, extractDir, target)
				if err != nil {
					os.RemoveAll(extractDir)
					advLog("error", "[UPLOAD] 构建刷写任务失败: %v", err)
					jsonErr(w, http.StatusBadRequest, err.Error())
					return
				}
				log.Printf("[UPLOAD] %s -> tar.gz 解压到 %s，%d 文件，mode=%s -> %s",
					safeName, extractDir, len(files), task.Mode, target)
				advLog("success", "[UPLOAD] 任务构建完成: mode=%s, 目标=%s, task_id=%s", task.Mode, target, task.ID)
				go doFlash(task)
				jsonOK(w, map[string]string{"task_id": task.ID, "message": msg})
				return
			}

			// ==== 情况2: tar.bz2 / tbz2 / tbz — 流式解压，不落盘压缩包 ====
			if kind == archiveTarBz2 {
				extractDir := filepath.Join(unpackDir, fmt.Sprintf("pkg_%d_tarbz2", time.Now().UnixNano()))
				if err := os.MkdirAll(extractDir, 0755); err != nil {
					jsonErr(w, http.StatusInternalServerError, "创建解压目录失败: "+err.Error())
					return
				}
				advLog("info", "[UPLOAD] 开始流式解压 tar.bz2 -> %s", extractDir)
				files, err := extractTarBz2(part, extractDir)
				if err != nil {
					os.RemoveAll(extractDir)
					advLog("error", "[UPLOAD] 解压 tar.bz2 失败: %v", err)
					jsonErr(w, http.StatusBadRequest, "解压 tar.bz2 失败: "+err.Error())
					return
				}
				if len(files) == 0 {
					os.RemoveAll(extractDir)
					advLog("error", "[UPLOAD] tar.bz2 包内没有常规文件")
					jsonErr(w, http.StatusBadRequest, "tar.bz2 包内没有常规文件")
					return
				}
				advLog("success", "[UPLOAD] tar.bz2 解压完成, 共 %d 个文件", len(files))
				for _, f := range files {
					if st, e := os.Stat(f); e == nil {
						advLog("data", "[UPLOAD] 解压文件: %s (%s)", filepath.Base(f), formatBytes(st.Size()))
					}
				}
				task, msg, err := buildTaskFromExtracted(files, extractDir, target)
				if err != nil {
					os.RemoveAll(extractDir)
					advLog("error", "[UPLOAD] 构建刷写任务失败: %v", err)
					jsonErr(w, http.StatusBadRequest, err.Error())
					return
				}
				log.Printf("[UPLOAD] %s -> tar.bz2 解压到 %s，%d 文件，mode=%s -> %s",
					safeName, extractDir, len(files), task.Mode, target)
				advLog("success", "[UPLOAD] 任务构建完成: mode=%s, 目标=%s, task_id=%s", task.Mode, target, task.ID)
				go doFlash(task)
				jsonOK(w, map[string]string{"task_id": task.ID, "message": msg})
				return
			}

			// ==== 情况3: zip / rar — 需先整体落盘再解压 ====
			if kind == archiveZip || kind == archiveRar {
				ext := ".zip"
				if kind == archiveRar {
					ext = ".rar"
				}
				archivePath = filepath.Join(uploadDir, fmt.Sprintf("pkg_%d%s", time.Now().UnixNano(), ext))
				advLog("info", "[UPLOAD] 接收压缩包 %s -> %s", ext, archivePath)
				dst, cerr := os.Create(archivePath)
				if cerr != nil {
					advLog("error", "[UPLOAD] 创建临时文件失败: %v", cerr)
					jsonErr(w, http.StatusInternalServerError, "创建临时文件失败: "+cerr.Error())
					return
				}
				written, err = io.Copy(dst, part)
				dst.Close()
				if err != nil {
					os.Remove(archivePath)
					advLog("error", "[UPLOAD] 保存压缩包失败: %v", err)
					jsonErr(w, http.StatusInternalServerError, "保存固件包失败: "+err.Error())
					return
				}
				if written == 0 {
					os.Remove(archivePath)
					advLog("error", "[UPLOAD] 压缩包内容为空")
					jsonErr(w, http.StatusBadRequest, "文件内容为空")
					return
				}
				advLog("success", "[UPLOAD] 压缩包接收完成: %s (%s)", filepath.Base(archivePath), formatBytes(written))
			} else {
				innerName, err = checkFirmwareName(safeName)
				if err != nil {
					advLog("error", "[UPLOAD] 文件类型检查失败: %v", err)
					jsonErr(w, http.StatusBadRequest, err.Error())
					return
				}
				head := make([]byte, 2)
				n, _ := io.ReadFull(part, head)
				head = head[:n]
				isGzip = n == 2 && head[0] == 0x1f && head[1] == 0x8b
				advLog("data", "[UPLOAD] 单文件固件: innerName=%s, isGzip=%v", innerName, isGzip)

				var src io.Reader = io.MultiReader(bytes.NewReader(head), part)
				if isGzip {
					gz, gerr := gzip.NewReader(src)
					if gerr != nil {
						advLog("error", "[UPLOAD] gzip 解压初始化失败: %v", gerr)
						jsonErr(w, http.StatusBadRequest, "gzip 解压失败: "+gerr.Error())
						return
					}
					defer gz.Close()
					src = gz
					advLog("info", "[UPLOAD] 检测到 gzip 格式, 将自动解压")
				}

				tmpPath = filepath.Join(uploadDir, fmt.Sprintf("fw_%d_%s", time.Now().UnixNano(), innerName))
				dst, cerr := os.Create(tmpPath)
				if cerr != nil {
					advLog("error", "[UPLOAD] 创建临时文件失败: %v", cerr)
					jsonErr(w, http.StatusInternalServerError, "创建临时文件失败: "+cerr.Error())
					return
				}
				written, err = io.Copy(dst, src)
				dst.Close()
				if err != nil {
					os.Remove(tmpPath)
					if isGzip {
						advLog("error", "[UPLOAD] gzip 解压写入失败: %v", err)
						jsonErr(w, http.StatusBadRequest, "gzip 解压失败: "+err.Error())
					} else {
						advLog("error", "[UPLOAD] 保存文件失败: %v", err)
						jsonErr(w, http.StatusInternalServerError, "保存文件失败: "+err.Error())
					}
					return
				}
				if written == 0 {
					os.Remove(tmpPath)
					advLog("error", "[UPLOAD] 文件内容为空")
					jsonErr(w, http.StatusBadRequest, "文件内容为空")
					return
				}
				advLog("success", "[UPLOAD] 固件文件已暂存: %s (%s), isGzip=%v", filepath.Base(tmpPath), formatBytes(written), isGzip)
			}
		}
		part.Close()
	}

	// 后续处理（zip / rar / 单文件）；tar.gz / tar.bz2 已在上面提前 return
	if kind != archiveZip && kind != archiveRar && tmpPath == "" {
		jsonErr(w, http.StatusBadRequest, "获取文件失败: 缺少 firmware 文件")
		return
	}
	if target == "" || !strings.HasPrefix(target, "/dev/") || strings.Contains(target, "..") {
		if kind == archiveZip || kind == archiveRar {
			os.Remove(archivePath)
		} else {
			os.Remove(tmpPath)
		}
		jsonErr(w, http.StatusBadRequest, "非法目标设备路径")
		return
	}

	var task *FlashTask
	var msg string

	if kind == archiveZip || kind == archiveRar {
		upSetStage("extracting")
		extractDir := filepath.Join(unpackDir, fmt.Sprintf("pkg_%d", time.Now().UnixNano()))
		if err := os.MkdirAll(extractDir, 0755); err != nil {
			os.Remove(archivePath)
			advLog("error", "[UPLOAD] 创建解压目录失败: %v", err)
			jsonErr(w, http.StatusInternalServerError, "创建解压目录失败: "+err.Error())
			return
		}

		advLog("info", "[UPLOAD] 开始解压 %s 压缩包 -> %s", kind, extractDir)
		var files []string
		if kind == archiveZip {
			files, err = extractZip(archivePath, extractDir)
		} else {
			files, err = extractRar(archivePath, extractDir)
		}
		if err != nil {
			os.RemoveAll(extractDir)
			advLog("error", "[UPLOAD] 解压 %s 失败: %v", kind, err)
			jsonErr(w, http.StatusBadRequest, fmt.Sprintf("解压 %s 固件包失败: %v", kind, err))
			return
		}
		advLog("success", "[UPLOAD] %s 解压完成, 共 %d 个文件", kind, len(files))
		for _, f := range files {
			if st, e := os.Stat(f); e == nil {
				advLog("data", "[UPLOAD] 解压文件: %s (%s)", filepath.Base(f), formatBytes(st.Size()))
			}
		}

		task, msg, err = buildTaskFromExtracted(files, extractDir, target)
		if err != nil {
			os.RemoveAll(extractDir)
			advLog("error", "[UPLOAD] 构建刷写任务失败: %v", err)
			jsonErr(w, http.StatusBadRequest, err.Error())
			return
		}
		log.Printf("[UPLOAD] %s -> 已解压到 %s，共 %d 个文件，mode=%s -> %s",
			safeName, extractDir, len(files), task.Mode, target)
		advLog("success", "[UPLOAD] 任务构建完成: mode=%s, 目标=%s, task_id=%s", task.Mode, target, task.ID)
	} else {
		task = newTask(target, written)
		task.Mode = "single"
		task.Items = []FlashWriteItem{{Name: innerName, Offset: 0, Size: written, Path: tmpPath}}

		msg = fmt.Sprintf("文件 %s (%d MB) 已暂存，正在刷写到 %s ...", safeName, written/1024/1024, target)
		if isGzip {
			msg = fmt.Sprintf("gzip 固件 %s 已自动解压为 %s (%d MB)，正在刷写到 %s ...",
				safeName, innerName, written/1024/1024, target)
		}
		log.Printf("[UPLOAD] %s -> %s (%d bytes, gzip=%v) -> %s", safeName, tmpPath, written, isGzip, target)
		advLog("success", "[UPLOAD] 单文件任务已创建: mode=single, 目标=%s, task_id=%s", target, task.ID)
	}

	go doFlash(task)

	jsonOK(w, map[string]string{
		"task_id": task.ID,
		"message": msg,
	})
}

// ============ Archive Extractors ============

// extractTarGz 流式解压 tar.gz 到 destDir，返回解出的常规文件路径列表
func extractTarGz(reader io.Reader, destDir string) ([]string, error) {
	gzReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("gzip 解压初始化失败: %w", err)
	}
	defer gzReader.Close()

	return extractTarStream(gzReader, destDir)
}

// extractTarBz2 流式解压 tar.bz2 到 destDir，返回解出的常规文件路径列表
func extractTarBz2(reader io.Reader, destDir string) ([]string, error) {
	bz2Reader := bzip2.NewReader(reader)
	return extractTarStream(bz2Reader, destDir)
}

// extractTarStream 是 tar 层的通用解压逻辑，供 gzip / bzip2 等上层解压器复用
func extractTarStream(tarStream io.Reader, destDir string) ([]string, error) {
	tarReader := tar.NewReader(tarStream)
	var files []string

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取 tar 条目失败: %w", err)
		}

		cleanName := filepath.Clean(header.Name)
		if cleanName == ".." || strings.HasPrefix(cleanName, "../") || filepath.IsAbs(cleanName) {
			log.Printf("extractTarStream: 跳过可疑路径: %s", header.Name)
			continue
		}
		cleanName = filepath.FromSlash(cleanName)
		outPath := filepath.Join(destDir, cleanName)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(outPath, os.FileMode(header.Mode)); err != nil {
				return nil, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
				return nil, err
			}
			mode := os.FileMode(header.Mode)
			if mode == 0 {
				mode = 0644
			}
			outFile, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return nil, err
			}
			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return nil, fmt.Errorf("写入 %s 失败: %w", outPath, err)
			}
			outFile.Close()
			files = append(files, outPath)
		}
	}
	return files, nil
}

// extractRar 解压 RAR 文件到 destDir，返回解出的常规文件路径列表
func extractRar(rarPath, destDir string) ([]string, error) {
	r, err := rardecode.OpenReader(rarPath)
	if err != nil {
		return nil, fmt.Errorf("打开 RAR 文件失败: %w", err)
	}
	defer r.Close()

	var files []string
	for {
		header, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取 RAR 条目失败: %w", err)
		}

		// 防御路径穿越
		cleanName := filepath.Clean(header.Name)
		if cleanName == ".." || strings.HasPrefix(cleanName, "../") || filepath.IsAbs(cleanName) {
			log.Printf("extractRar: 跳过可疑路径: %s", header.Name)
			continue
		}
		cleanName = filepath.FromSlash(cleanName)
		outPath := filepath.Join(destDir, cleanName)

		if header.IsDir {
			if err := os.MkdirAll(outPath, 0755); err != nil {
				return nil, err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			return nil, err
		}

		outFile, err := os.Create(outPath)
		if err != nil {
			return nil, err
		}
		if _, err := io.Copy(outFile, r); err != nil {
			outFile.Close()
			return nil, fmt.Errorf("写入 %s 失败: %w", outPath, err)
		}
		outFile.Close()
		files = append(files, outPath)
	}
	return files, nil
}

// extractZip 解压 ZIP 文件到 destDir，返回解出的常规文件路径列表
func extractZip(zipPath, destDir string) ([]string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var files []string
	for _, f := range r.File {
		cleanName := filepath.Clean(f.Name)
		if cleanName == ".." || strings.HasPrefix(cleanName, "../") || filepath.IsAbs(cleanName) {
			log.Printf("extractZip: 跳过可疑路径: %s", f.Name)
			continue
		}
		outPath := filepath.Join(destDir, cleanName)

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(outPath, 0755); err != nil {
				return nil, err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			return nil, err
		}

		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		out, err := os.Create(outPath)
		if err != nil {
			rc.Close()
			return nil, err
		}
		_, err = io.Copy(out, rc)
		out.Close()
		rc.Close()
		if err != nil {
			return nil, err
		}
		files = append(files, outPath)
	}
	return files, nil
}

// ============ Task Building From Extracted Files ============

// buildTaskFromExtracted 根据解压后的文件列表构建刷写任务。
//
//   - 只有一个文件：直接整盘写入目标设备。
//   - 多个文件：交由 buildMultiFileTask 处理（package-file/config.cfg + parameter.txt）。
func buildTaskFromExtracted(files []string, extractDir, target string) (*FlashTask, string, error) {
	if len(files) == 0 {
		return nil, "", fmt.Errorf("固件包内没有解压出任何文件")
	}

	if len(files) == 1 {
		return buildSingleFileTask(files[0], extractDir, target)
	}

	return buildMultiFileTask(files, extractDir, target)
}

// buildSingleFileTask 解压结果只有一个文件时，直接将其整盘写入目标设备
func buildSingleFileTask(filePath, extractDir, target string) (*FlashTask, string, error) {
	st, err := os.Stat(filePath)
	if err != nil {
		return nil, "", fmt.Errorf("读取解压文件失败: %v", err)
	}

	task := newTask(target, st.Size())
	task.Mode = "single"
	task.Items = []FlashWriteItem{{
		Name: filepath.Base(filePath), Offset: 0,
		Size: st.Size(), Path: filePath,
	}}

	msg := fmt.Sprintf("固件包内只有单一文件 %s (%d MB)，将整盘写入 %s ...",
		filepath.Base(filePath), st.Size()/1024/1024, target)
	return task, msg, nil
}

// buildMultiFileTask 处理解压结果包含多个文件时的刷写逻辑。
//
// 支持两种多文件固件包格式，均需要 parameter.txt 分区表（mtdparts，offset/size 单位为 512
// 字节扇区）配合使用，按“分区名 -> 镜像文件路径”的映射逐一写入对应偏移：
//
//  1. package-file + parameter.txt（Rockchip update.img / afptool 风格）：
//     package-file 是一个文本文件，每行 "<名称> <相对路径>"，将分区/组件名映射到固件包内的
//     相对文件路径，例如 "boot Image/boot.img"。
//
//  2. config.cfg + parameter.txt（RKDevTool 风格）：
//     config.cfg 是 RKDevTool 使用的二进制配置文件（"CFG" 魔数开头），记录镜像名称、路径及是否
//     勾选写入，通过 parseConfigCfg / findImageInCfg 解析。
//
// 若固件包内既没有 package-file 也没有 config.cfg，则退化为按分区名直接猜测文件名
// （findImageByGuess，如分区名 "boot" 对应文件 "boot.img"）。
//
// parameter.txt 中未能匹配到任何镜像文件的分区会被跳过（这是正常情况，因为分区表里常常包含
// 一些仅用于占位、不随固件包分发独立镜像的分区）；只有当所有分区都匹配失败时才报错。
func buildMultiFileTask(files []string, extractDir, target string) (*FlashTask, string, error) {
	advLog("info", "[MULTI] 开始解析多文件固件包: %d 个文件, 解压目录=%s, 目标=%s", len(files), extractDir, target)
	paramPath := findByBaseNames(files, []string{"parameter.txt", "parameter"})
	if paramPath == "" {
		advLog("error", "[MULTI] 未找到 parameter.txt 分区表")
		return nil, "", fmt.Errorf("固件包内包含 %d 个文件，但未找到 parameter.txt 分区表，无法确定多文件写入方式", len(files))
	}
	advLog("data", "[MULTI] 找到分区表: %s", paramPath)
	partitions, err := parseParameterFile(paramPath)
	if err != nil {
		advLog("error", "[MULTI] 解析 parameter.txt 失败: %v", err)
		return nil, "", fmt.Errorf("解析 parameter.txt 失败: %v", err)
	}
	advLog("success", "[MULTI] parameter.txt 解析完成: %d 个分区定义", len(partitions))
	for _, p := range partitions {
		advLog("data", "[MULTI] 分区: '%s' offset=0x%X(扇区%d) size=0x%X uuid=%s", p.Name, p.Offset, p.Offset, p.Size, p.UUID)
	}

	// 2. 定位镜像描述文件：优先 package-file，其次 config.cfg，都没有则按文件名猜测
	var pkgItems []PackageFileItem
	var cfgItems []CfgImageItem
	var sourceDesc string

	if pkgPath := findByBaseNames(files, []string{"package-file"}); pkgPath != "" {
		pkgItems, err = parsePackageFile(pkgPath)
		if err != nil {
			advLog("error", "[MULTI] 解析 package-file 失败: %v", err)
			return nil, "", fmt.Errorf("解析 package-file 失败: %v", err)
		}
		sourceDesc = "package-file"
		advLog("data", "[MULTI] 镜像描述来源: package-file (%s), %d 条条目", pkgPath, len(pkgItems))
	} else if cfgPath := findConfigCfg(files); cfgPath != "" {
		cfgItems, err = parseConfigCfg(cfgPath)
		if err != nil {
			advLog("error", "[MULTI] 解析 config.cfg 失败: %v", err)
			return nil, "", fmt.Errorf("解析 config.cfg 失败: %v", err)
		}
		sourceDesc = "config.cfg"
		advLog("data", "[MULTI] 镜像描述来源: config.cfg (%s), %d 条条目", cfgPath, len(cfgItems))
	} else {
		sourceDesc = "文件名猜测"
		advLog("warn", "[MULTI] 未找到 package-file 或 config.cfg, 使用文件名猜测模式")
	}

	// 3. 按分区表逐一匹配镜像文件，构建写入任务
	var items []FlashWriteItem
	var totalSize int64
	var skipped []string

	for _, part := range partitions {
		if part.Name == "" {
			continue
		}

		var imgPath string
		var found bool

		switch {
		case pkgItems != nil:
			imgPath, found = findImageInPackage(pkgItems, files, extractDir, part.Name)
		case cfgItems != nil:
			imgPath, found = findImageInCfg(cfgItems, files, extractDir, part.Name)
		default:
			imgPath, found = findImageByGuess(files, part.Name)
		}

		if !found {
			skipped = append(skipped, part.Name)
			advLog("warn", "[MULTI] 分区 '%s' 未匹配到镜像文件, 跳过", part.Name)
			continue
		}

		st, serr := os.Stat(imgPath)
		if serr != nil || !st.Mode().IsRegular() || st.Size() == 0 {
			skipped = append(skipped, part.Name)
			if serr != nil {
				log.Printf("buildMultiFileTask: stat '%s' 失败: %v", imgPath, serr)
				advLog("warn", "[MULTI] stat '%s' 失败: %v", imgPath, serr)
			} else {
				advLog("warn", "[MULTI] 分区 '%s' 文件无效或为空, 跳过", part.Name)
			}
			continue
		}

		offsetBytes := int64(part.Offset) * paramSectorSize
		items = append(items, FlashWriteItem{
			Name:   part.Name,
			Offset: offsetBytes,
			Size:   st.Size(),
			Path:   imgPath,
		})
		totalSize += st.Size()
		advLog("success", "[MULTI] 分区 '%s' -> %s (%s) @ offset 0x%X", part.Name, filepath.Base(imgPath), formatBytes(st.Size()), offsetBytes)
	}

	if len(items) == 0 {
		advLog("error", "[MULTI] 未匹配到任何镜像文件 (来源: %s)", sourceDesc)
		return nil, "", fmt.Errorf("根据 parameter.txt 的 %d 个分区，未能匹配到任何可写入的镜像文件（描述文件来源: %s）", len(partitions), sourceDesc)
	}

	task := newTask(target, totalSize)
	task.Mode = "multi"
	task.Items = items
	task.Partitions = partitions
	advLog("success", "[MULTI] 多文件任务构建完成: %d 个分区匹配, %d 个跳过, 总大小 %s, task_id=%s",
		len(items), len(skipped), formatBytes(totalSize), task.ID)

	msg := fmt.Sprintf("固件包解析完成（%s + parameter.txt），%d 个分区匹配到镜像（%d 个分区未匹配，已跳过），共 %d MB，将重建 GPT 分区表后写入 %s ...",
		sourceDesc, len(items), len(skipped), totalSize/1024/1024, target)
	if len(skipped) > 0 {
		log.Printf("[MULTI] 以下分区未找到对应镜像文件，已跳过: %v", skipped)
	}
	for _, it := range items {
		log.Printf("[MULTI] 分区 '%s' -> %s (%d bytes) @ offset 0x%X", it.Name, it.Path, it.Size, it.Offset)
	}

	return task, msg, nil
}

// findByBaseName 在文件列表中按文件名查找（大小写不敏感）
func findByBaseName(files []string, name string) string {
	for _, f := range files {
		if strings.EqualFold(filepath.Base(f), name) {
			return f
		}
	}
	return ""
}

// findByBaseNames 在文件列表中按一组候选文件名依次查找（大小写不敏感），返回第一个命中的路径
func findByBaseNames(files []string, names []string) string {
	for _, n := range names {
		if p := findByBaseName(files, n); p != "" {
			return p
		}
	}
	return ""
}

// findConfigCfg 在文件列表中查找 RKDevTool 风格的 config.cfg。
// 优先按常见文件名 "config.cfg" 查找并用魔数校验；若未命中，再遍历全部文件用魔数 "CFG" 识别，
// 因为不同厂商打包时该文件名可能不同。
func findConfigCfg(files []string) string {
	if p := findByBaseName(files, "config.cfg"); p != "" && isConfigCfg(p) {
		return p
	}
	for _, f := range files {
		if isConfigCfg(f) {
			return f
		}
	}
	return ""
}

// doFlash 执行刷写
func doFlash(task *FlashTask) {
	defer func() {
		if t := getTask(task.ID); t != nil && t.Status == "error" {
			log.Printf("[%s] ❌ Flash failed: %s", task.ID, t.Error)
			advLog("error", "[%s] ❌ 刷写失败: %s", task.ID, t.Error)
		}
	}()

	log.Printf("[%s] Flash start: mode=%s, %d item(s) -> %s (%d bytes)",
		task.ID, task.Mode, len(task.Items), task.TargetDev, task.Total)
	advLog("info", "[%s] 🚀 刷写开始: 模式=%s, %d 个分区 -> %s (总大小 %s)",
		task.ID, task.Mode, len(task.Items), task.TargetDev, formatBytes(task.Total))
	for i, item := range task.Items {
		advLog("data", "[%s] 📋 分区 #%d: '%s' (%s) @ offset 0x%X, 文件: %s",
			task.ID, i+1, item.Name, formatBytes(item.Size), item.Offset, item.Path)
	}

	if task.Mode == "multi" && len(task.Partitions) > 0 {
		updateTask(task.ID, func(t *FlashTask) { t.Status = "partitioning" })
		advLog("warn", "[%s] 🔧 重建 GPT 分区表: %s (共 %d 个分区定义)",
			task.ID, task.TargetDev, len(task.Partitions))
		if err := applyGPTPartitionTable(task.TargetDev, task.Partitions); err != nil {
			updateTask(task.ID, func(t *FlashTask) {
				t.Status = "error"
				t.Error = "重建 GPT 分区表失败: " + err.Error()
			})
			advLog("error", "[%s] ❌ 重建 GPT 分区表失败: %v", task.ID, err)
			return
		}
		advLog("success", "[%s] ✅ GPT 分区表重建完成", task.ID)
		updateTask(task.ID, func(t *FlashTask) { t.Status = "writing" })
	}

	dst, err := os.OpenFile(task.TargetDev, os.O_WRONLY|os.O_SYNC, 0)
	if err != nil {
		updateTask(task.ID, func(t *FlashTask) {
			t.Status = "error"
			t.Error = "打开目标设备失败: " + err.Error()
		})
		advLog("error", "[%s] ❌ 打开目标设备 %s 失败: %v", task.ID, task.TargetDev, err)
		return
	}
	defer dst.Close()

	buf := make([]byte, blockSize)
	var totalWritten int64
	start := time.Now()

	for _, item := range task.Items {
		src, err := os.Open(item.Path)
		if err != nil {
			updateTask(task.ID, func(t *FlashTask) {
				t.Status = "error"
				t.Error = fmt.Sprintf("打开源文件 %s 失败: %v", item.Name, err)
			})
			advLog("error", "[%s] ❌ 打开源文件 %s 失败: %v", task.ID, item.Path, err)
			return
		}

		advLog("info", "[%s] 📝 开始写入分区 '%s' (%s) @ offset 0x%X",
			task.ID, item.Name, formatBytes(item.Size), item.Offset)

		if _, err := dst.Seek(item.Offset, io.SeekStart); err != nil {
			src.Close()
			updateTask(task.ID, func(t *FlashTask) {
				t.Status = "error"
				t.Error = fmt.Sprintf("定位分区 '%s' 失败: %v", item.Name, err)
			})
			advLog("error", "[%s] ❌ 定位分区 '%s' @ offset 0x%X 失败: %v", task.ID, item.Name, item.Offset, err)
			return
		}

		log.Printf("[%s] Writing '%s' (%d bytes) @ offset %d", task.ID, item.Name, item.Size, item.Offset)

		var itemWritten int64
		lastLogTime := time.Now()
		for {
			n, readErr := src.Read(buf)
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					src.Close()
					updateTask(task.ID, func(t *FlashTask) {
						t.Status = "error"
						t.Error = fmt.Sprintf("写入 '%s' 失败 @ %d bytes: %v", item.Name, totalWritten, werr)
					})
					advLog("error", "[%s] ❌ 写入分区 '%s' 失败 @ %d bytes: %v", task.ID, item.Name, totalWritten, werr)
					return
				}
				totalWritten += int64(n)
				itemWritten += int64(n)
				elapsed := time.Since(start).Seconds()
				updateTask(task.ID, func(t *FlashTask) {
					t.Written = totalWritten
					if t.Total > 0 {
						t.Progress = float64(totalWritten) / float64(t.Total) * 100
					}
					if elapsed > 0 {
						t.Speed = float64(totalWritten) / elapsed
					}
				})
				if time.Since(lastLogTime) >= 2*time.Second {
					advLog("data", "[%s] ⏳ 分区 '%s' 写入进度: %s / %s (%.1f%%), 速度 %.1f MB/s",
						task.ID, item.Name, formatBytes(itemWritten), formatBytes(item.Size),
						float64(itemWritten)/float64(item.Size)*100,
						float64(totalWritten)/elapsed/1024/1024)
					lastLogTime = time.Now()
				}
			}
			if readErr != nil {
				if readErr != io.EOF {
					src.Close()
					updateTask(task.ID, func(t *FlashTask) {
						t.Status = "error"
						t.Error = fmt.Sprintf("读取 '%s' 失败: %v", item.Name, readErr)
					})
					advLog("error", "[%s] ❌ 读取分区 '%s' 文件失败: %v", task.ID, item.Name, readErr)
					return
				}
				break
			}
		}
		src.Close()
		advLog("success", "[%s] ✅ 分区 '%s' 写入完成 (%s)", task.ID, item.Name, formatBytes(itemWritten))
	}

	updateTask(task.ID, func(t *FlashTask) { t.Status = "syncing" })
	advLog("warn", "[%s] 💾 同步磁盘数据中...", task.ID)
	dst.Sync()
	advLog("success", "[%s] ✅ 磁盘同步完成", task.ID)

	elapsed := time.Since(start).Seconds()
	updateTask(task.ID, func(t *FlashTask) {
		t.Status = "done"
		t.Progress = 100
		t.Written = t.Total
		if elapsed > 0 {
			t.Speed = float64(t.Total) / elapsed
		}
	})
	log.Printf("[%s] ✅ Done: %d bytes, %.1fs, %.1f MB/s",
		task.ID, totalWritten, elapsed, float64(totalWritten)/elapsed/1024/1024)
	advLog("success", "[%s] 🎉 刷写全部完成! 总计 %s, 耗时 %.1fs, 平均速度 %.1f MB/s",
		task.ID, formatBytes(totalWritten), elapsed, float64(totalWritten)/elapsed/1024/1024)
}

// ============ GPT Partition Table Rebuild (sgdisk) ============

// blockDeviceSectors 读取目标块设备的实际容量（单位：512 字节扇区），通过
// /sys/class/block/<dev>/size 获取，避免依赖外部工具解析。
func blockDeviceSectors(target string) (uint64, error) {
	name := strings.TrimPrefix(target, "/dev/")
	data, err := os.ReadFile(filepath.Join("/sys/class/block", name, "size"))
	if err != nil {
		return 0, fmt.Errorf("读取 %s 容量失败: %w", target, err)
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("解析 %s 容量失败: %w", target, err)
	}
	return v, nil
}

// applyGPTPartitionTable 依据 parameter.txt 中的分区表，在目标设备上按磁盘当前实际容量
// 重新创建一份 GPT 分区表（通过 sgdisk 完成）。
//
// 这是多文件刷写（package-file/config.cfg + parameter.txt）流程的必要前置步骤：如果不重建
// 分区表，直接按偏移量写入镜像会导致设备上残留旧的、与当前磁盘容量不匹配的 GPT
// （典型症状如 fdisk 报 "GPT PMBR size mismatch"、"backup GPT table is corrupt"、
// "backup GPT table is not on the end of the device"），系统重启后可能无法正确识别分区。
//
// 流程：
//  1. sgdisk --zap-all：销毁设备上现有的 GPT / MBR 数据结构（即使已损坏也能处理）；
//  2. sgdisk -o -a 1 -n ... -c ... -u ...：新建空 GPT（-o），关闭对齐（-a 1，保证分区起始
//     扇区与 parameter.txt 完全一致），并逐个创建分区、设置名称及（若 parameter.txt 中通过
//     "uuid:<分区名>=<UUID>" 指定）强制的 PARTUUID。
//
// 分区表项的起止扇区直接取自 parameter.txt（单位为 512 字节扇区）；size 为 0xFFFFFFFF
// （parameter.txt 中以 "-" 表示，通常是最后一个分区，如 rootfs/userdata）的分区，结束扇区
// 传 0，由 sgdisk 自动延伸到最后一个可用扇区（并为备份 GPT 预留空间）。
// 起始扇区落在 GPT 保留区（前 gptReservedSectors 个扇区）内的分区不纳入 GPT 管理，仍按原始
// 偏移直接写入（doFlash 中的逐项写入逻辑本身不受影响）。
func applyGPTPartitionTable(target string, partitions []ParamPartition) error {
	sgdiskPath, err := exec.LookPath("sgdisk")
	if err != nil {
		advLog("error", "[GPT] 未找到 sgdisk 工具")
		return fmt.Errorf("未找到 sgdisk 工具，无法重建 GPT 分区表（请安装 gdisk / gptfdisk 软件包）: %w", err)
	}
	advLog("data", "[GPT] sgdisk 路径: %s", sgdiskPath)

	diskSectors, err := blockDeviceSectors(target)
	if err != nil {
		advLog("error", "[GPT] 获取磁盘容量失败: %v", err)
		return err
	}
	if diskSectors == 0 {
		advLog("error", "[GPT] 磁盘容量为 0")
		return fmt.Errorf("无法获取 %s 的磁盘容量", target)
	}
	advLog("data", "[GPT] 磁盘 %s 容量: %d 扇区 (%s)", target, diskSectors, formatBytes(int64(diskSectors)*512))

	args, created := buildSgdiskArgs(partitions, diskSectors)
	if len(created) == 0 {
		advLog("error", "[GPT] 没有可用于创建 GPT 分区的有效分区项")
		return fmt.Errorf("parameter.txt 中没有可用于创建 GPT 分区的有效分区项")
	}
	advLog("data", "[GPT] 将创建 %d 个 GPT 分区: %v", len(created), created)

	advLog("data", "[GPT] 执行 sgdisk --zap-all %s", target)
	zapOut, zapErr := exec.Command(sgdiskPath, "--zap-all", target).CombinedOutput()
	if zapErr != nil {
		log.Printf("[GPT] sgdisk --zap-all %s 返回错误（忽略，继续重建）: %v, output: %s", target, zapErr, string(zapOut))
		advLog("warn", "[GPT] sgdisk --zap-all 返回错误(忽略): %v", zapErr)
	} else {
		advLog("data", "[GPT] sgdisk --zap-all 完成, 已清除旧分区表")
	}

	fullArgs := append([]string{"-o", "-a", "1"}, args...)
	fullArgs = append(fullArgs, target)
	advLog("data", "[GPT] 执行: sgdisk %s", strings.Join(fullArgs, " "))
	out, err := exec.Command(sgdiskPath, fullArgs...).CombinedOutput()
	if err != nil {
		advLog("error", "[GPT] sgdisk 写入分区表失败: %v, 输出: %s", err, string(out))
		return fmt.Errorf("sgdisk 写入分区表失败: %v, output: %s\n命令: sgdisk %s", err, string(out), strings.Join(fullArgs, " "))
	}

	rereadPartitionTable(target)
	advLog("success", "[GPT] GPT 分区表重建完成: %s, %d 个分区, 磁盘 %d 扇区", target, len(created), diskSectors)
	log.Printf("[GPT] 已通过 sgdisk 在 %s 上重建 GPT 分区表，共 %d 个分区: %v，磁盘容量 %d 扇区\n命令: sgdisk %s\nsgdisk 输出:\n%s",
		target, len(created), created, diskSectors, strings.Join(fullArgs, " "), string(out))
	return nil
}

// buildSgdiskArgs 依据 parameter.txt 分区表及磁盘实际容量，生成 sgdisk 的分区相关参数
// （-n / -c / -u），并返回实际创建的分区名列表。分区编号按创建顺序从 1 开始连续编号。
//
// 每个分区生成形如：
//
//	-n 1:64:6207            起始扇区:结束扇区（结束扇区为 0 表示延伸到最后一个可用扇区）
//	-c 1:uboot              分区名称
//	-u 1:614e0000-...       （可选）强制指定分区 GUID，即内核命令行中的 PARTUUID
func buildSgdiskArgs(partitions []ParamPartition, diskSectors uint64) ([]string, []string) {
	var args []string
	var created []string

	// GPT 最后一个可用扇区（备份 GPT 位于磁盘末尾 gptBackupSectors 个扇区内）
	var lastUsable uint64
	if diskSectors > gptBackupSectors+1 {
		lastUsable = diskSectors - gptBackupSectors - 1
	}

	for _, p := range partitions {
		name := p.Name
		if idx := strings.Index(name, ":"); idx >= 0 {
			name = name[:idx]
		}
		if name == "" {
			continue
		}
		if uint64(p.Offset) < gptReservedSectors {
			log.Printf("[GPT] 分区 '%s' 起始扇区 %d 落在 GPT 保留区内（<%d），跳过 GPT 分区创建，仍按原始偏移写入",
				name, p.Offset, gptReservedSectors)
			continue
		}
		if uint64(p.Offset) > lastUsable {
			log.Printf("[GPT] 分区 '%s' 起始扇区 %d 超出磁盘可用范围（最后可用扇区 %d），跳过", name, p.Offset, lastUsable)
			continue
		}

		// 结束扇区：size 为 "-" 或超出磁盘可用范围时传 0，让 sgdisk 自动延伸到最后可用扇区
		var end uint64
		if p.Size != 0xFFFFFFFF && p.Size != 0 {
			e := uint64(p.Offset) + uint64(p.Size) - 1
			if e <= lastUsable {
				end = e
			}
		}

		num := len(created) + 1
		args = append(args,
			"-n", fmt.Sprintf("%d:%d:%d", num, p.Offset, end),
			"-c", fmt.Sprintf("%d:%s", num, name),
		)

		if p.UUID != "" {
			if isValidGUID(p.UUID) {
				args = append(args, "-u", fmt.Sprintf("%d:%s", num, p.UUID))
				log.Printf("[GPT] 分区 '%s' (#%d) 强制 PARTUUID = %s", name, num, p.UUID)
			} else {
				log.Printf("[GPT] 分区 '%s' 的 UUID %q 格式非法，忽略，将使用随机 PARTUUID", name, p.UUID)
			}
		}

		created = append(created, name)
	}

	return args, created
}

// isValidGUID 校验形如 xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx 的 GUID 字符串
func isValidGUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
	}
	return true
}

// rereadPartitionTable 通知内核重新读取目标设备的分区表，优先使用 partprobe，
// 不可用时回退到 blockdev --rereadpt；都不可用时仅记录日志，不视为致命错误
// （分区表本身已经正确写入设备，内核视图会在下次插拔/重启后自动刷新）。
func rereadPartitionTable(target string) {
	if p, err := exec.LookPath("partprobe"); err == nil {
		if out, err := exec.Command(p, target).CombinedOutput(); err != nil {
			log.Printf("[GPT] partprobe %s 失败: %v, output: %s", target, err, string(out))
		}
		return
	}
	if p, err := exec.LookPath("blockdev"); err == nil {
		if out, err := exec.Command(p, "--rereadpt", target).CombinedOutput(); err != nil {
			log.Printf("[GPT] blockdev --rereadpt %s 失败: %v, output: %s", target, err, string(out))
		}
		return
	}
	log.Printf("[GPT] 未找到 partprobe/blockdev，内核分区表视图可能需要重新插拔设备或重启后才能刷新")
}

// ============ parameter.txt parsing ============

type ParamPartition struct {
	Name   string
	Offset uint32
	Size   uint32
	UUID   string // 来自 parameter.txt 中 "uuid:<分区名>=<UUID>" 行，为空表示不强制
}

func parseParameterFile(path string) ([]ParamPartition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseParameterBytes(string(data))
}

// partBaseName 返回分区名去掉 ":grow" 等冒号后缀后的部分
func partBaseName(name string) string {
	if idx := strings.Index(name, ":"); idx >= 0 {
		return name[:idx]
	}
	return name
}

// parseUUIDLine 解析形如 "uuid:rootfs=614e0000-0000-4b53-8000-1d28000054a9" 的行，
// 返回分区名与 UUID。不是 uuid 行时 ok 为 false。
func parseUUIDLine(line string) (name, uuid string, ok bool) {
	if len(line) < 5 || !strings.EqualFold(line[:5], "uuid:") {
		return "", "", false
	}
	rest := line[5:]
	eq := strings.Index(rest, "=")
	if eq < 0 {
		return "", "", false
	}
	name = strings.TrimSpace(rest[:eq])
	uuid = strings.TrimSpace(rest[eq+1:])
	if name == "" || uuid == "" {
		return "", "", false
	}
	return name, uuid, true
}

func parseParameterBytes(content string) ([]ParamPartition, error) {
	var result []ParamPartition
	uuidMap := make(map[string]string) // 分区名(小写) -> UUID

	content = strings.ReplaceAll(content, "\r\n", "\n")
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// uuid:<分区名>=<UUID> 行：记录需要强制指定的 PARTUUID
		if n, u, ok := parseUUIDLine(line); ok {
			uuidMap[strings.ToLower(partBaseName(n))] = u
			continue
		}

		idx := strings.Index(line, "mtdparts")
		if idx < 0 {
			continue
		}
		colonIdx := strings.Index(line[idx:], ":")
		if colonIdx < 0 {
			continue
		}
		partsStr := line[idx+colonIdx+1:]
		for _, entry := range strings.Split(partsStr, ",") {
			if p, ok := parsePartitionEntry(entry); ok {
				result = append(result, p)
			}
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("未在文件中找到 mtdparts 分区信息")
	}

	// 将 uuid 映射应用到对应分区
	used := make(map[string]bool)
	for i := range result {
		key := strings.ToLower(partBaseName(result[i].Name))
		if u, ok := uuidMap[key]; ok {
			result[i].UUID = u
			used[key] = true
		}
	}
	for k, u := range uuidMap {
		if !used[k] {
			log.Printf("parseParameterBytes: uuid:%s=%s 没有对应的分区，已忽略", k, u)
		}
	}

	return result, nil
}

func parsePartitionEntry(entry string) (ParamPartition, bool) {
	entry = strings.TrimSpace(entry)
	atIdx := strings.Index(entry, "@")
	if atIdx < 0 {
		return ParamPartition{}, false
	}
	sizeStr := strings.TrimSpace(entry[:atIdx])
	rest := entry[atIdx+1:]

	parenIdx := strings.Index(rest, "(")
	if parenIdx < 0 {
		return ParamPartition{}, false
	}
	offsetStr := strings.TrimSpace(rest[:parenIdx])
	rest2 := rest[parenIdx+1:]

	closeIdx := strings.Index(rest2, ")")
	if closeIdx < 0 {
		return ParamPartition{}, false
	}
	name := strings.TrimSpace(rest2[:closeIdx])
	if name == "" {
		return ParamPartition{}, false
	}

	var size uint32
	if strings.Contains(sizeStr, "-") {
		size = 0xFFFFFFFF
	} else {
		v, err := parseHex32(sizeStr)
		if err != nil {
			return ParamPartition{}, false
		}
		size = v
	}

	offset, err := parseHex32(offsetStr)
	if err != nil {
		return ParamPartition{}, false
	}

	return ParamPartition{Name: name, Offset: offset, Size: size}, true
}

func parseHex32(s string) (uint32, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimPrefix(s, "0x")
	v, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(v), nil
}

// ============ package-file parsing (Rockchip update.img / afptool 风格) ============

// PackageFileItem 表示 package-file 中的一条镜像条目：名称 -> 固件包内相对路径
type PackageFileItem struct {
	Name string
	Path string
}

// parsePackageFile 解析 Rockchip package-file。
//
// 典型格式（"#" 开头为注释，空行忽略，每行由任意空白分隔为“名称 路径”两列）：
//
//	# NAME			PATH
//	package-file		package-file
//	parameter		parameter.txt
//	bootloader		MiniLoaderAll.bin
//	boot			boot.img
//	rootfs			rootfs.img
//
// 名称对应 parameter.txt 分区表中的分区名，路径是相对于固件包解压目录的相对路径
// （也可能带有子目录，如 "Image/boot.img"）。
func parsePackageFile(path string) ([]PackageFileItem, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parsePackageFileBytes(string(data))
}

func parsePackageFileBytes(content string) ([]PackageFileItem, error) {
	var items []PackageFileItem
	content = strings.ReplaceAll(content, "\r\n", "\n")
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := strings.TrimPrefix(fields[0], "/")
		p := strings.ReplaceAll(fields[1], "\\", "/")
		items = append(items, PackageFileItem{Name: name, Path: p})
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("package-file 内没有解析出任何有效条目")
	}
	return items, nil
}

// findImageInPackage 在 package-file 条目中按分区名查找对应镜像文件的实际路径。
// partName 可能带有形如 "boot:grow" 的后缀（增长标记等），仅取冒号前的部分用于匹配。
func findImageInPackage(items []PackageFileItem, files []string, extractDir, partName string) (string, bool) {
	name := partName
	if idx := strings.Index(name, ":"); idx >= 0 {
		name = name[:idx]
	}

	for _, it := range items {
		if !strings.EqualFold(it.Name, name) {
			continue
		}
		if it.Path == "" {
			return "", false
		}

		p := it.Path
		if !filepath.IsAbs(p) {
			p = filepath.Join(extractDir, p)
		}
		if st, err := os.Stat(p); err == nil && st.Mode().IsRegular() {
			return p, true
		}

		// 路径本身可能因不同打包工具而与解压目录结构不完全一致，回退按文件名匹配
		base := filepath.Base(it.Path)
		for _, f := range files {
			if strings.EqualFold(filepath.Base(f), base) {
				return f, true
			}
		}
		log.Printf("findImageInPackage: '%s' -> '%s' 文件不存在", partName, it.Path)
		return "", false
	}
	return "", false
}

// ============ config.cfg parsing (RKDevTool 风格) ============

const (
	cfgHeaderSize  = 29
	cfgNameUnits   = 40
	cfgPathUnits   = 260
	cfgMinItemSize = 2 + cfgNameUnits*2 + cfgPathUnits*2 + 4 + 1
	cfgAddrAuto    = 0xFFFFFFFF
)

type CfgImageItem struct {
	Name     string
	Path     string
	Address  uint32
	Selected bool
}

func isConfigCfg(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	magic := make([]byte, 4)
	n, _ := io.ReadFull(f, magic)
	return n == 4 && string(magic[:3]) == "CFG"
}

func parseConfigCfg(path string) ([]CfgImageItem, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < cfgHeaderSize {
		return nil, fmt.Errorf("文件太小，读不到完整 header")
	}
	if string(data[0:3]) != "CFG" {
		return nil, fmt.Errorf("magic 不匹配，不是 config.cfg")
	}

	begin := binary.LittleEndian.Uint32(data[23:27])
	itemSize := binary.LittleEndian.Uint16(data[27:29])

	if itemSize < cfgMinItemSize {
		return nil, fmt.Errorf("itemSize=%d 过小（至少 %d）", itemSize, cfgMinItemSize)
	}
	if int(begin) < cfgHeaderSize || int(begin) > len(data) {
		return nil, fmt.Errorf("begin=0x%X 超出文件范围", begin)
	}

	payload := len(data) - int(begin)
	entries := payload / int(itemSize)
	if remainder := payload % int(itemSize); remainder != 0 {
		log.Printf("parseConfigCfg: 末尾多出 %d 字节", remainder)
	}

	items := make([]CfgImageItem, 0, entries)
	off := int(begin)
	for i := 0; i < entries; i++ {
		if off+int(itemSize) > len(data) {
			break
		}
		rec := data[off : off+int(itemSize)]
		off += int(itemSize)

		selfSize := binary.LittleEndian.Uint16(rec[0:2])
		if selfSize != itemSize {
			log.Printf("parseConfigCfg: item %d size(%d) != header.itemSize(%d)", i, selfSize, itemSize)
		}

		nameBytes := rec[2 : 2+cfgNameUnits*2]
		pathBytes := rec[2+cfgNameUnits*2 : 2+cfgNameUnits*2+cfgPathUnits*2]
		addrOff := 2 + cfgNameUnits*2 + cfgPathUnits*2
		address := binary.LittleEndian.Uint32(rec[addrOff : addrOff+4])
		selected := rec[addrOff+4] != 0

		name := utf16leToString(nameBytes)
		p := strings.ReplaceAll(utf16leToString(pathBytes), "\\", "/")

		items = append(items, CfgImageItem{Name: name, Path: p, Address: address, Selected: selected})
	}
	return items, nil
}

func utf16leToString(b []byte) string {
	u16 := make([]uint16, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		v := uint16(b[i]) | uint16(b[i+1])<<8
		if v == 0 {
			break
		}
		u16 = append(u16, v)
	}
	return string(utf16.Decode(u16))
}

func findImageInCfg(items []CfgImageItem, files []string, extractDir, partName string) (string, bool) {
	name := partName
	if idx := strings.Index(name, ":"); idx >= 0 {
		name = name[:idx]
	}

	for _, it := range items {
		if !strings.EqualFold(it.Name, name) {
			continue
		}
		if !it.Selected {
			log.Printf("findImageInCfg: '%s' 未勾选，跳过", partName)
			return "", false
		}
		if it.Path == "" {
			return "", false
		}

		p := it.Path
		if !filepath.IsAbs(p) {
			p = filepath.Join(extractDir, p)
		}
		if st, err := os.Stat(p); err == nil && st.Mode().IsRegular() {
			return p, true
		}

		base := filepath.Base(it.Path)
		for _, f := range files {
			if strings.EqualFold(filepath.Base(f), base) {
				return f, true
			}
		}
		log.Printf("findImageInCfg: '%s' -> '%s' 文件不存在", partName, it.Path)
		return "", false
	}
	return "", false
}

func findImageByGuess(files []string, partName string) (string, bool) {
	name := partName
	if idx := strings.Index(name, ":"); idx >= 0 {
		name = name[:idx]
	}
	candidates := []string{name, name + ".img", name + ".raw", "_" + name + ".img"}

	for _, f := range files {
		base := filepath.Base(f)
		for _, c := range candidates {
			if strings.EqualFold(base, c) {
				return f, true
			}
		}
	}
	return "", false
}

// ============ SSE Progress ============

func handleProgress(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("id")
	if taskID == "" {
		jsonErr(w, http.StatusBadRequest, "缺少 task id")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	for {
		task := getTask(taskID)
		if task == nil {
			fmt.Fprintf(w, "data: {\"status\":\"error\",\"error\":\"task not found\"}\n\n")
			flusher.Flush()
			return
		}
		data, _ := json.Marshal(task)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
		if task.Status == "done" || task.Status == "error" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ============ Device Filter ============

func getFilteredDevices() []BlockDevice {
	cmd := exec.Command("lsblk", "-bdnJ", "-o", "NAME,SIZE,TYPE,MODEL,TRAN")
	output, err := cmd.Output()
	if err != nil {
		log.Printf("lsblk failed: %v", err)
		return getMockDevices()
	}

	var data LsblkOutput
	if err := json.Unmarshal(output, &data); err != nil {
		log.Printf("json unmarshal failed: %v", err)
		return getMockDevices()
	}

	var result []BlockDevice
	for _, d := range data.BlockDevices {
		if strings.HasPrefix(d.Name, "loop") || strings.HasPrefix(d.Name, "ram") ||
			strings.HasPrefix(d.Name, "dm-") || strings.HasPrefix(d.Name, "sr") ||
			strings.HasPrefix(d.Name, "zram") {
			continue
		}
		if !strings.HasPrefix(d.Name, "sd") && !strings.HasPrefix(d.Name, "nvme") &&
			!strings.HasPrefix(d.Name, "mmcblk") {
			continue
		}

		tran := ""
		if d.Tran != nil {
			tran = *d.Tran
		}

		dt := "Disk"
		if strings.HasPrefix(d.Name, "nvme") {
			dt = "NVMe"
		} else if strings.HasPrefix(d.Name, "mmcblk") {
			dt = "eMMC/SD"
		} else if tran == "usb" {
			dt = "USB"
		} else if tran == "sata" || tran == "scsi" {
			dt = "SATA/SCSI"
		}

		sizeBytes, _ := d.Size.Int64()
		model := ""
		if d.Model != nil {
			model = *d.Model
		}

		result = append(result, BlockDevice{
			Name: d.Name, Path: "/dev/" + d.Name,
			Type: dt, Size: formatBytes(sizeBytes), Model: model,
		})
	}

	if len(result) == 0 {
		return getMockDevices()
	}
	return result
}

func getMockDevices() []BlockDevice {
	return []BlockDevice{}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b <= 0 {
		return "0 B"
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}

// ============ Storage Detection (smartctl / sgdisk / lsblk) ============

type StoragePartition struct {
	Number int    `json:"number"`
	Start  string `json:"start"`
	End    string `json:"end"`
	Size   string `json:"size"`
	Code   string `json:"code"`
	Name   string `json:"name"`
	Type   string `json:"type"`
}

type StorageSmart struct {
	Available bool              `json:"available"`
	Model     string            `json:"model"`
	Serial    string            `json:"serial"`
	Firmware  string            `json:"firmware"`
	Capacity  string            `json:"capacity"`
	Temp      string            `json:"temp"`
	Hours     string            `json:"hours"`
	PowerCycles string          `json:"powerCycles"`
	Health    string            `json:"health"`
	Protocol  string            `json:"protocol"`
	Attrs     []SmartAttr       `json:"attrs"`
	NvmeAttrs []NvmeSmartAttr   `json:"nvmeAttrs"`
	RawFull   string            `json:"rawFull"`
	Error     string            `json:"error,omitempty"`
}

type SmartAttr struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Value string `json:"value"`
	Worst string `json:"worst"`
	Thresh string `json:"thresh"`
	Raw   string `json:"raw"`
}

type NvmeSmartAttr struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type StorageDevice struct {
	Device     string             `json:"device"`
	Model      string             `json:"model"`
	Size       string             `json:"size"`
	Type       string             `json:"type"`
	Transport  string             `json:"transport"`
	Partitions []StoragePartition `json:"partitions"`
	Smart      StorageSmart       `json:"smart"`
}

type StorageResponse struct {
	Devices []StorageDevice `json:"devices"`
	Error   string          `json:"error,omitempty"`
}

type CpuInfo struct {
	Model       string  `json:"model"`
	Cores       int     `json:"cores"`
	Kernel      string  `json:"kernel"`
	Uptime      string  `json:"uptime"`
	LoadAvg1    float64 `json:"loadAvg1"`
	LoadAvg5    float64 `json:"loadAvg5"`
	LoadAvg15   float64 `json:"loadAvg15"`
	CpuUsage    float64 `json:"cpuUsage"`
	MemTotal    int64   `json:"memTotal"`
	MemFree     int64   `json:"memFree"`
	MemUsed     int64   `json:"memUsed"`
	MemUsage    float64 `json:"memUsage"`
	SwapTotal   int64   `json:"swapTotal"`
	SwapFree    int64   `json:"swapFree"`
	SwapUsed    int64   `json:"swapUsed"`
	Temp        string  `json:"temp"`
	Arch        string  `json:"arch"`
	Hostname    string  `json:"hostname"`
}

type NetInterface struct {
	Name      string `json:"name"`
	Ip        string `json:"ip"`
	Mac       string `json:"mac"`
	Speed     string `json:"speed"`
	Status    string `json:"status"`
	Type      string `json:"type"`
	RxBytes   int64  `json:"rxBytes"`
	TxBytes   int64  `json:"txBytes"`
	Link      string `json:"link"`
}

type NetworkInfo struct {
	Interfaces []NetInterface `json:"interfaces"`
	Error      string         `json:"error,omitempty"`
}

func handleCpu(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	info := CpuInfo{}

	out, err := os.ReadFile("/proc/cpuinfo")
	if err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(line, "model name") || strings.HasPrefix(line, "Hardware") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					info.Model = strings.TrimSpace(parts[1])
					break
				}
			}
		}
	}

	out, err = os.ReadFile("/proc/cpuinfo")
	if err == nil {
		count := 0
		for _, line := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(line, "processor") {
				count++
			}
		}
		info.Cores = count
	}

	out, _ = os.ReadFile("/proc/sys/kernel/osrelease")
	info.Kernel = strings.TrimSpace(string(out))

	out, _ = os.ReadFile("/proc/uptime")
	if len(out) > 0 {
		fields := strings.Fields(string(out))
		if len(fields) > 0 {
			upSecs, _ := strconv.ParseFloat(fields[0], 64)
			days := int(upSecs) / 86400
			hours := (int(upSecs) % 86400) / 3600
			mins := (int(upSecs) % 3600) / 60
			if days > 0 {
				info.Uptime = fmt.Sprintf("%d天 %d小时 %d分钟", days, hours, mins)
			} else if hours > 0 {
				info.Uptime = fmt.Sprintf("%d小时 %d分钟", hours, mins)
			} else {
				info.Uptime = fmt.Sprintf("%d分钟", mins)
			}
		}
	}

	out, _ = os.ReadFile("/proc/loadavg")
	if len(out) > 0 {
		fields := strings.Fields(string(out))
		if len(fields) >= 3 {
			info.LoadAvg1, _ = strconv.ParseFloat(fields[0], 64)
			info.LoadAvg5, _ = strconv.ParseFloat(fields[1], 64)
			info.LoadAvg15, _ = strconv.ParseFloat(fields[2], 64)
		}
	}

	out, _ = os.ReadFile("/proc/stat")
	if len(out) > 0 {
		lines := strings.Split(string(out), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "cpu ") {
				fields := strings.Fields(line)
				if len(fields) >= 5 {
					user, _ := strconv.ParseInt(fields[1], 10, 64)
					nice, _ := strconv.ParseInt(fields[2], 10, 64)
					system, _ := strconv.ParseInt(fields[3], 10, 64)
					idle, _ := strconv.ParseInt(fields[4], 10, 64)
					total := user + nice + system + idle
					if total > 0 {
						info.CpuUsage = float64(total-idle) / float64(total) * 100
					}
				}
				break
			}
		}
	}

	out, _ = os.ReadFile("/proc/meminfo")
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				info.MemTotal, _ = strconv.ParseInt(fields[1], 10, 64)
				info.MemTotal *= 1024
			}
		} else if strings.HasPrefix(line, "MemFree:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				info.MemFree, _ = strconv.ParseInt(fields[1], 10, 64)
				info.MemFree *= 1024
			}
		} else if strings.HasPrefix(line, "SwapTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				info.SwapTotal, _ = strconv.ParseInt(fields[1], 10, 64)
				info.SwapTotal *= 1024
			}
		} else if strings.HasPrefix(line, "SwapFree:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				info.SwapFree, _ = strconv.ParseInt(fields[1], 10, 64)
				info.SwapFree *= 1024
			}
		}
	}
	info.MemUsed = info.MemTotal - info.MemFree
	if info.MemTotal > 0 {
		info.MemUsage = float64(info.MemUsed) / float64(info.MemTotal) * 100
	}
	info.SwapUsed = info.SwapTotal - info.SwapFree

	temps, _ := filepath.Glob("/sys/class/thermal/thermal_zone*/temp")
	for _, tpath := range temps {
		data, err := os.ReadFile(tpath)
		if err != nil {
			continue
		}
		val := strings.TrimSpace(string(data))
		tempMilli, _ := strconv.ParseInt(val, 10, 64)
		if tempMilli > 0 {
			info.Temp = fmt.Sprintf("%.1f°C", float64(tempMilli)/1000.0)
			break
		}
	}

	out, _ = os.ReadFile("/proc/sys/kernel/arch") 
	if len(out) > 0 {
		info.Arch = strings.TrimSpace(string(out))
	}

	out, _ = os.ReadFile("/etc/hostname")
	info.Hostname = strings.TrimSpace(string(out))

	json.NewEncoder(w).Encode(info)
}

func handleNetwork(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var interfaces []NetInterface

	out, err := exec.Command("ip", "-o", "addr", "show").Output()
	if err != nil {
		json.NewEncoder(w).Encode(NetworkInfo{Error: "ip 命令执行失败: " + err.Error()})
		return
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		name := fields[1]
		if name == "lo" {
			continue
		}

		ip := ""
		for i := 3; i < len(fields)-1; i++ {
			if fields[i] == "inet" {
				ip = fields[i+1]
				if idx := strings.Index(ip, "/"); idx > 0 {
					ip = ip[:idx]
				}
				break
			}
		}

		mac := ""
		macOut, _ := exec.Command("cat", "/sys/class/net/"+name+"/address").Output()
		mac = strings.TrimSpace(string(macOut))

		operstate := "down"
		stateOut, _ := os.ReadFile("/sys/class/net/" + name + "/operstate")
		if len(stateOut) > 0 {
			operstate = strings.TrimSpace(string(stateOut))
		}
		status := "已连接"
		if operstate != "up" {
			status = "未连接"
		}

		netType := "以太网"
		if strings.HasPrefix(name, "wlan") || strings.HasPrefix(name, "wlp") {
			netType = "无线"
		} else if strings.HasPrefix(name, "usb") {
			netType = "USB"
		} else if strings.HasPrefix(name, "eth") || strings.HasPrefix(name, "en") {
			netType = "以太网"
		}

		speed := ""
		speedOut, _ := os.ReadFile("/sys/class/net/" + name + "/speed")
		if len(speedOut) > 0 {
			s := strings.TrimSpace(string(speedOut))
			if s != "" && s != "Unknown" {
				speed = s + " Mbps"
			}
		}

		rxBytes := int64(0)
		txBytes := int64(0)
		rxOut, _ := os.ReadFile("/sys/class/net/" + name + "/statistics/rx_bytes")
		if len(rxOut) > 0 {
			rxBytes, _ = strconv.ParseInt(strings.TrimSpace(string(rxOut)), 10, 64)
		}
		txOut, _ := os.ReadFile("/sys/class/net/" + name + "/statistics/tx_bytes")
		if len(txOut) > 0 {
			txBytes, _ = strconv.ParseInt(strings.TrimSpace(string(txOut)), 10, 64)
		}

		link := ""
		if operstate == "up" {
			link = "UP"
		} else {
			link = "DOWN"
		}

		interfaces = append(interfaces, NetInterface{
			Name:    name,
			Ip:      ip,
			Mac:     mac,
			Speed:   speed,
			Status:  status,
			Type:    netType,
			RxBytes: rxBytes,
			TxBytes: txBytes,
			Link:    link,
		})
	}

	json.NewEncoder(w).Encode(NetworkInfo{Interfaces: interfaces})
}

func handleStorage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		jsonErr(w, http.StatusMethodNotAllowed, "仅支持 GET")
		return
	}

	devices, err := detectStorageDevices()
	resp := StorageResponse{Devices: devices}
	if err != nil {
		resp.Error = err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func detectStorageDevices() ([]StorageDevice, error) {
	var devices []StorageDevice

	out, err := exec.Command("lsblk", "-b", "-d", "-n", "-o", "NAME,SIZE,MODEL,ROTA,TRAN").Output()
	if err != nil {
		return nil, fmt.Errorf("lsblk 执行失败: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		name := fields[0]
		if strings.HasPrefix(name, "sr") || strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "zram") {
			continue
		}

		sizeBytes, _ := strconv.ParseInt(fields[1], 10, 64)
		rota := fields[2]
		transport := ""
		if len(fields) >= 5 {
			transport = fields[4]
		}
		model := ""
		if len(fields) >= 4 {
			model = strings.Join(fields[3:len(fields)-1], " ")
			if len(fields) >= 5 {
				model = strings.Join(fields[3:], " ")
			}
		}

		devType := "HDD"
		if rota == "0" {
			devType = "SSD"
		}
		if transport == "usb" {
			devType = "USB"
		}

		device := "/dev/" + name
		dev := StorageDevice{
			Device:    device,
			Model:     model,
			Size:      formatBytes(sizeBytes),
			Type:      devType,
			Transport: transport,
		}

		dev.Partitions = getPartitions(device)
		dev.Smart = getSmartInfo(device)

		devices = append(devices, dev)
	}

	if len(devices) == 0 {
		return nil, fmt.Errorf("未检测到存储设备")
	}
	return devices, nil
}

func getPartitions(device string) []StoragePartition {
	out, err := exec.Command("sgdisk", "-p", device).Output()
	if err != nil {
		out2, err2 := exec.Command("fdisk", "-l", device).Output()
		if err2 != nil {
			return nil
		}
		return parseFdiskOutput(string(out2))
	}
	return parseSgdiskOutput(string(out))
}

func parseSgdiskOutput(output string) []StoragePartition {
	var parts []StoragePartition
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Disk") || strings.HasPrefix(line, "Number") || strings.Contains(line, "invalid") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		num, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		code := ""
		name := ""
		ptype := ""
		for i, f := range fields {
			if f == "code" && i+1 < len(fields) {
				code = fields[i+1]
			}
			if f == "name" && i+1 < len(fields) {
				name = fields[i+1]
			}
			if f == "type" && i+1 < len(fields) {
				ptype = fields[i+1]
			}
		}
		if code == "" && len(fields) >= 7 {
			code = fields[5]
		}
		if name == "" && len(fields) >= 8 {
			name = fields[6]
		}
		parts = append(parts, StoragePartition{
			Number: num,
			Start:  fields[1],
			End:    fields[2],
			Size:   fields[3] + " " + fields[4],
			Code:   code,
			Name:   name,
			Type:   ptype,
		})
	}
	return parts
}

func parseFdiskOutput(output string) []StoragePartition {
	var parts []StoragePartition
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "/dev/") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		numStr := fields[0]
		re := regexp.MustCompile(`\D`)
		numStr = re.ReplaceAllString(numStr, "")
		num, err := strconv.Atoi(numStr)
		if err != nil {
			num = len(parts) + 1
		}
		parts = append(parts, StoragePartition{
			Number: num,
			Start:  fields[1],
			End:    fields[2],
			Size:   fields[3] + " sectors",
			Code:   "",
			Name:   "",
			Type:   fields[4],
		})
	}
	return parts
}

func getSmartInfo(device string) StorageSmart {
	info := StorageSmart{Available: false}

	out, err := exec.Command("smartctl", "-a", device).CombinedOutput()
	text := string(out)
	info.RawFull = text

	if err != nil {
		if len(out) > 0 {
			info.Available = true
		}
		info.Error = fmt.Sprintf("smartctl 退出码非零: %v", err)
	} else {
		info.Available = true
	}

	if !info.Available {
		return info
	}

	info.Protocol = "SATA"
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Model Family:") || strings.HasPrefix(line, "Device Model:") || strings.HasPrefix(line, "Model Number:") {
			info.Model = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
		if strings.HasPrefix(line, "Serial Number:") {
			info.Serial = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
		if strings.HasPrefix(line, "Firmware Version:") {
			info.Firmware = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
		if strings.HasPrefix(line, "User Capacity:") {
			cap := strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
			if idx := strings.Index(cap, "["); idx >= 0 {
				cap = strings.TrimSpace(cap[:idx]) + " " + strings.TrimSpace(cap[idx:])
			}
			info.Capacity = cap
		}
		if strings.HasPrefix(line, "NVMe Version:") {
			info.Protocol = "NVMe"
		}
		if strings.HasPrefix(line, "Temperature:") {
			info.Temp = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
		if strings.HasPrefix(line, "Power On Hours:") {
			info.Hours = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
		if strings.HasPrefix(line, "Power Cycles:") {
			info.PowerCycles = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
		if strings.HasPrefix(line, "Power_On_Hours") {
			parts := strings.Fields(line)
			if len(parts) >= 10 {
				info.Hours = parts[9]
			}
		}
		if strings.HasPrefix(line, "Power_Cycle_Count") {
			parts := strings.Fields(line)
			if len(parts) >= 10 {
				info.PowerCycles = parts[9]
			}
		}
		if strings.HasPrefix(line, "SMART overall-health") || strings.HasPrefix(line, "SMART Health Status") {
			if strings.Contains(line, "PASSED") || strings.Contains(line, "OK") {
				info.Health = "PASSED"
			} else if strings.Contains(line, "FAILED") {
				info.Health = "FAILED"
			} else if idx := strings.Index(line, ":"); idx >= 0 {
				info.Health = strings.TrimSpace(line[idx+1:])
			}
		}
	}

	if info.Protocol == "NVMe" {
		info.NvmeAttrs = parseNvmeSmart(text)
	} else {
		info.Attrs = parseSmartAttrs(text)
	}
	return info
}

func parseNvmeSmart(text string) []NvmeSmartAttr {
	var attrs []NvmeSmartAttr
	inSection := false
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "SMART/Health Information") {
			inSection = true
			continue
		}
		if inSection {
			if trimmed == "" {
				if len(attrs) > 0 {
					break
				}
				continue
			}
			if strings.HasPrefix(trimmed, "Error Information") || strings.HasPrefix(trimmed, "Self-test Log") || strings.HasPrefix(trimmed, "Supported Power") || strings.HasPrefix(trimmed, "Supported LBA") {
				break
			}
			idx := strings.Index(trimmed, ":")
			if idx < 0 {
				continue
			}
			key := strings.TrimSpace(trimmed[:idx])
			val := strings.TrimSpace(trimmed[idx+1:])
			if key == "" || val == "" {
				continue
			}
			if strings.Contains(key, "Comp. Temp. Threshold") && val == "" {
				continue
			}
			attrs = append(attrs, NvmeSmartAttr{Key: key, Value: val})
		}
	}
	return attrs
}

func parseSmartAttrs(text string) []SmartAttr {
	var attrs []SmartAttr
	inAttrs := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ID# ATTRIBUTE_NAME") {
			inAttrs = true
			continue
		}
		if inAttrs {
			if line == "" {
				if len(attrs) > 0 {
					break
				}
				continue
			}
			r := regexp.MustCompile(`^\d`)
			if !r.MatchString(line) {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 10 {
				continue
			}
			id, err := strconv.Atoi(fields[0])
			if err != nil {
				continue
			}
			attrs = append(attrs, SmartAttr{
				ID:    id,
				Name:  fields[1],
				Value: fields[2],
				Worst: fields[3],
				Thresh: fields[4],
				Raw:   strings.Join(fields[9:], " "),
			})
		}
	}
	return attrs
}

// ============ Update Helper ============

const (
	githubReleaseAPI = "https://api.github.com/repos/yifengyou/aiot-3588ied/releases/tags/spi_recovery_uboot2026"
	updateTmpFile    = "/tmp/update.img"
	updateTargetDev  = "/dev/mtdblock0"
)

type GithubAsset struct {
	Name               string `json:"name"`
	Size               int64  `json:"size"`
	BrowserDownloadURL string `json:"browser_download_url"`
	UpdatedAt          string `json:"updated_at"`
	Timestamp          string `json:"timestamp,omitempty"`
}

type GithubRelease struct {
	TagName  string        `json:"tag_name"`
	Name     string        `json:"name"`
	Body     string        `json:"body"`
	Assets   []GithubAsset `json:"assets"`
}

func handleUpdateCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	proxy := r.URL.Query().Get("proxy")

	asset, version, allAssets, _, err := findLatestSpiImage(githubReleaseAPI, proxy)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"hasUpdate":      false,
			"latestVersion":  "",
			"currentVersion": buildTime,
			"downloadUrl":    "",
			"allAssets":      []interface{}{},
			"versionCount":   0,
			"error":          err.Error(),
		})
		return
	}

	// build version list with download URLs
	type versionItem struct {
		Version     string `json:"version"`
		DownloadUrl string `json:"downloadUrl"`
		Size        int64  `json:"size"`
	}
	var versionList []versionItem
	for _, a := range allAssets {
		versionList = append(versionList, versionItem{
			Version:     a.Timestamp,
			DownloadUrl: a.BrowserDownloadURL,
			Size:        a.Size,
		})
	}

	// build version strings for backward compat
	var allVersions []string
	for _, a := range allAssets {
		allVersions = append(allVersions, a.Timestamp)
	}

	// buildTime format: "2026.09.29", version format: "20260927"
	currentNorm := strings.ReplaceAll(buildTime, ".", "")
	currentInt, _ := strconv.ParseInt(currentNorm, 10, 64)
	versionInt, _ := strconv.ParseInt(version, 10, 64)
	hasUpdate := version != "" && versionInt > currentInt

	json.NewEncoder(w).Encode(map[string]interface{}{
		"hasUpdate":      hasUpdate,
		"latestVersion":  version,
		"currentVersion": buildTime,
		"downloadUrl":    asset.BrowserDownloadURL,
		"downloadSize":   asset.Size,
		"allVersions":    allVersions,
		"allAssets":      versionList,
		"versionCount":   len(allAssets),
		"error":          "",
	})
}

func findLatestSpiImage(apiURL, proxy string) (*GithubAsset, string, []GithubAsset, []string, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: tr}

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, "", nil, nil, fmt.Errorf("创建请求失败: %v", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "rkdev-update-checker")

	resp, err := client.Do(req)
	if err != nil {
		if proxy != "" {
			proxiedURL := proxy + apiURL
			req2, err2 := http.NewRequest("GET", proxiedURL, nil)
			if err2 != nil {
				return nil, "", nil, nil, fmt.Errorf("代理请求创建失败: %v", err2)
			}
			req2.Header.Set("Accept", "application/vnd.github+json")
			req2.Header.Set("User-Agent", "rkdev-update-checker")
			resp, err = client.Do(req2)
		}
		if err != nil {
			return nil, "", nil, nil, fmt.Errorf("请求 GitHub API 失败: %v", err)
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, "", nil, nil, fmt.Errorf("GitHub API 返回状态码: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", nil, nil, fmt.Errorf("读取响应失败: %v", err)
	}

	var release GithubRelease
	if err := json.Unmarshal(body, &release); err != nil {
		return nil, "", nil, nil, fmt.Errorf("解析 JSON 失败: %v", err)
	}

	var bestAsset *GithubAsset
	var bestTimestamp string
	var allAssets []GithubAsset

	for i := range release.Assets {
		asset := &release.Assets[i]
		name := asset.Name

		var timestamp string
		if strings.HasPrefix(name, "spi_full_disk_") && strings.HasSuffix(name, ".img") {
			middle := strings.TrimPrefix(name, "spi_full_disk_")
			middle = strings.TrimSuffix(middle, ".img")
			if len(middle) == 8 && isAllDigits(middle) {
				timestamp = middle
			}
		}

		if timestamp == "" {
			continue
		}

		asset.Timestamp = timestamp
		allAssets = append(allAssets, *asset)

		if bestTimestamp == "" || timestamp > bestTimestamp {
			bestTimestamp = timestamp
			bestAsset = asset
		}
	}

	if bestAsset == nil {
		return nil, "", nil, nil, fmt.Errorf("未找到 spi_full_disk_*.img 镜像文件")
	}

	// sort allAssets by timestamp descending
	sort.Slice(allAssets, func(i, j int) bool {
		return allAssets[i].Timestamp > allAssets[j].Timestamp
	})

	var allVersions []string
	for _, a := range allAssets {
		allVersions = append(allVersions, a.Timestamp)
	}

	version := bestTimestamp
	return bestAsset, version, allAssets, allVersions, nil
}

func isAllDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ============ Update Background Task ============

type UpdateProgress struct {
	mu       sync.Mutex
	Phase    string  `json:"phase"`
	Percent  float64 `json:"percent"`
	Written  int64   `json:"written"`
	Total    int64   `json:"total"`
	Speed    float64 `json:"speed"`
	Eta      float64 `json:"eta"`
	Done     bool    `json:"done"`
	Error    string  `json:"error"`
	Success  bool    `json:"success"`
	Message  string  `json:"message"`
	Running  bool    `json:"running"`
	cancelCh chan struct{}
}

var updateProgress UpdateProgress

func startUpdateTask(downloadURL string) {
	updateProgress.mu.Lock()
	if updateProgress.Running {
		updateProgress.mu.Unlock()
		return
	}
	updateProgress.Running = true
	updateProgress.Done = false
	updateProgress.Error = ""
	updateProgress.Success = false
	updateProgress.Phase = "downloading"
	updateProgress.Percent = 0
	updateProgress.Written = 0
	updateProgress.Total = 0
	updateProgress.cancelCh = make(chan struct{})
	updateProgress.mu.Unlock()

	go func() {
		defer func() {
			updateProgress.mu.Lock()
			updateProgress.Running = false
			updateProgress.mu.Unlock()
		}()

		advLog("info", "开始下载更新镜像: " + downloadURL)

		client := &http.Client{
			Timeout: 30 * time.Minute,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		}

		currentURL := downloadURL
		var resp *http.Response
		for redirectCount := 0; redirectCount < 10; redirectCount++ {
			req, err := http.NewRequest("GET", currentURL, nil)
			if err != nil {
				updateProgress.mu.Lock()
				updateProgress.Error = "创建请求失败: " + err.Error()
				updateProgress.mu.Unlock()
				advLog("error", "创建请求失败: %v", err)
				return
			}
			req.Header.Set("User-Agent", "rkdev-updater")
			resp, err = client.Do(req)
			if err != nil {
				updateProgress.mu.Lock()
				updateProgress.Error = "下载失败: " + err.Error()
				updateProgress.mu.Unlock()
				advLog("error", "下载失败: %v", err)
				return
			}
			if resp.StatusCode == 301 || resp.StatusCode == 302 || resp.StatusCode == 307 || resp.StatusCode == 308 {
				loc := resp.Header.Get("Location")
				resp.Body.Close()
				if loc == "" {
					updateProgress.mu.Lock()
					updateProgress.Error = fmt.Sprintf("重定向响应缺少 Location (状态码: %d)", resp.StatusCode)
					updateProgress.mu.Unlock()
					return
				}
				if !strings.HasPrefix(loc, "http") {
					base, _ := url.Parse(currentURL)
					loc = base.Scheme + "://" + base.Host + loc
				}
				advLog("info", "重定向 (%d) -> %s", resp.StatusCode, loc)
				currentURL = loc
				continue
			}
			break
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			updateProgress.mu.Lock()
			updateProgress.Error = fmt.Sprintf("下载返回状态码: %d", resp.StatusCode)
			updateProgress.mu.Unlock()
			return
		}

		totalSize := resp.ContentLength
		tmpPath := updateTmpFile
		f, err := os.Create(tmpPath)
		if err != nil {
			updateProgress.mu.Lock()
			updateProgress.Error = "创建临时文件失败: " + err.Error()
			updateProgress.mu.Unlock()
			return
		}

		buf := make([]byte, 256*1024)
		var written int64
		startTime := time.Now()
		lastReport := startTime

		for {
			select {
			case <-updateProgress.cancelCh:
				f.Close()
				os.Remove(tmpPath)
				updateProgress.mu.Lock()
				updateProgress.Error = "下载已取消"
				updateProgress.Running = false
				updateProgress.mu.Unlock()
				advLog("info", "下载已取消")
				return
			default:
			}
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				wn, werr := f.Write(buf[:n])
				written += int64(wn)
				now := time.Now()
				if now.Sub(lastReport) >= 500*time.Millisecond || (readErr != nil && written > 0) {
					elapsed := now.Sub(startTime).Seconds()
					speed := float64(0)
					if elapsed > 0 {
						speed = float64(written) / elapsed
					}
					percent := float64(0)
					if totalSize > 0 {
						percent = float64(written) / float64(totalSize) * 100
					}
					eta := float64(0)
					if speed > 0 && totalSize > 0 {
						eta = float64(totalSize-written) / speed
					}
					updateProgress.mu.Lock()
					updateProgress.Phase = "downloading"
					updateProgress.Percent = percent
					updateProgress.Written = written
					updateProgress.Total = totalSize
					updateProgress.Speed = speed
					updateProgress.Eta = eta
					updateProgress.mu.Unlock()
					lastReport = now
				}
				if werr != nil {
					f.Close()
					updateProgress.mu.Lock()
					updateProgress.Error = "写入文件失败: " + werr.Error()
					updateProgress.mu.Unlock()
					return
				}
			}
			if readErr != nil {
				break
			}
		}
		f.Close()

		advLog("info", "下载完成: %s (%d 字节)", tmpPath, written)

		updateProgress.mu.Lock()
		updateProgress.Phase = "flashing"
		updateProgress.Percent = 0
		updateProgress.Written = 0
		updateProgress.Total = written
		updateProgress.Speed = 0
		updateProgress.Eta = 0
		updateProgress.mu.Unlock()

		advLog("info", "开始同步写入 " + updateTargetDev)
		devFile, err := os.OpenFile(updateTargetDev, os.O_WRONLY|os.O_SYNC, 0)
		if err != nil {
			updateProgress.mu.Lock()
			updateProgress.Error = "打开设备失败: " + err.Error()
			updateProgress.mu.Unlock()
			return
		}
		defer devFile.Close()

		srcFile, err := os.Open(tmpPath)
		if err != nil {
			updateProgress.mu.Lock()
			updateProgress.Error = "打开下载文件失败: " + err.Error()
			updateProgress.mu.Unlock()
			return
		}
		defer srcFile.Close()

		flashBuf := make([]byte, 4*1024)
		var writtenDev int64
		flashStart := time.Now()

		for {
			n, readErr := srcFile.Read(flashBuf)
			if n > 0 {
				wn, werr := devFile.Write(flashBuf[:n])
				writtenDev += int64(wn)
				now := time.Now()
				elapsed := now.Sub(flashStart).Seconds()
				speed := float64(0)
				if elapsed > 0 {
					speed = float64(writtenDev) / elapsed
				}
				percent := float64(0)
				if written > 0 {
					percent = float64(writtenDev) / float64(written) * 100
				}
				eta := float64(0)
				if speed > 0 && written > 0 {
					eta = float64(written-writtenDev) / speed
				}
				updateProgress.mu.Lock()
				updateProgress.Phase = "flashing"
				updateProgress.Percent = percent
				updateProgress.Written = writtenDev
				updateProgress.Total = written
				updateProgress.Speed = speed
				updateProgress.Eta = eta
				updateProgress.mu.Unlock()
				if werr != nil {
					devFile.Close()
					updateProgress.mu.Lock()
					updateProgress.Error = "写入设备失败: " + werr.Error()
					updateProgress.mu.Unlock()
					advLog("error", "写入设备失败: %v", werr)
					return
				}
			}
			if readErr != nil {
				break
			}
		}
		devFile.Close()

		advLog("info", "写入完成: %s (%d 字节)", updateTargetDev, writtenDev)
		os.Remove(tmpPath)

		updateProgress.mu.Lock()
		updateProgress.Done = true
		updateProgress.Success = true
		updateProgress.Message = "更新已写入 SPI Flash，重启设备后生效"
		updateProgress.Percent = 100
		updateProgress.mu.Unlock()
	}()
}

func handleUpdateCancel(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	updateProgress.mu.Lock()
	if updateProgress.Phase == "flashing" {
		updateProgress.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "error": "写入固件阶段不可取消"})
		return
	}
	if updateProgress.Running && updateProgress.cancelCh != nil {
		close(updateProgress.cancelCh)
		updateProgress.cancelCh = nil
	}
	updateProgress.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func handleUpdateDownload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "请求参数解析失败: " + err.Error()})
		return
	}

	downloadURL := req.URL
	if downloadURL == "" {
		asset, _, _, _, err := findLatestSpiImage(githubReleaseAPI, "")
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "检测更新失败: " + err.Error()})
			return
		}
		downloadURL = asset.BrowserDownloadURL
	}

	if downloadURL == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "无有效下载地址"})
		return
	}

	startUpdateTask(downloadURL)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "不支持 SSE"})
		return
	}

	for {
		updateProgress.mu.Lock()
		data := map[string]interface{}{
			"phase":   updateProgress.Phase,
			"percent": updateProgress.Percent,
			"written": updateProgress.Written,
			"total":   updateProgress.Total,
			"speed":   updateProgress.Speed,
			"eta":     updateProgress.Eta,
			"running": updateProgress.Running,
			"done":    updateProgress.Done,
			"success": updateProgress.Success,
			"error":   updateProgress.Error,
			"message": updateProgress.Message,
		}
		done := updateProgress.Done || updateProgress.Error != ""
		running := updateProgress.Running
		updateProgress.mu.Unlock()

		event := "progress"
		if data["error"].(string) != "" {
			event = "error"
		} else if data["done"].(bool) {
			event = "done"
		}

		jsonBytes, _ := json.Marshal(data)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, jsonBytes)
		flusher.Flush()

		if done || !running {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func handleUpdateUpload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	r.ParseMultipartForm(100 << 20)

	file, header, err := r.FormFile("file")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "读取上传文件失败: " + err.Error()})
		return
	}
	defer file.Close()

	advLog("info", "收到上传更新文件: " + header.Filename + " (" + formatBytes(header.Size) + ")")

	tmpPath := updateTmpFile
	f, err := os.Create(tmpPath)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "创建临时文件失败: " + err.Error()})
		return
	}

	written, err := io.Copy(f, file)
	f.Close()
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "保存文件失败: " + err.Error()})
		return
	}

	advLog("info", "上传文件已保存: %s (%d 字节)", tmpPath, written)

	updateProgress.mu.Lock()
	if updateProgress.Running {
		updateProgress.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "已有更新任务正在执行中"})
		return
	}
	updateProgress.Running = true
	updateProgress.Done = false
	updateProgress.Error = ""
	updateProgress.Success = false
	updateProgress.Phase = "flashing"
	updateProgress.Percent = 0
	updateProgress.Written = 0
	updateProgress.Total = written
	updateProgress.Speed = 0
	updateProgress.Eta = 0
	updateProgress.mu.Unlock()

	go func() {
		defer func() {
			updateProgress.mu.Lock()
			updateProgress.Running = false
			updateProgress.mu.Unlock()
		}()

		advLog("info", "开始同步写入 " + updateTargetDev)
		devFile, err := os.OpenFile(updateTargetDev, os.O_WRONLY|os.O_SYNC, 0)
		if err != nil {
			updateProgress.mu.Lock()
			updateProgress.Error = "打开设备失败: " + err.Error()
			updateProgress.mu.Unlock()
			return
		}
		defer devFile.Close()

		srcFile, err := os.Open(tmpPath)
		if err != nil {
			updateProgress.mu.Lock()
			updateProgress.Error = "打开上传文件失败: " + err.Error()
			updateProgress.mu.Unlock()
			return
		}
		defer srcFile.Close()

		flashBuf := make([]byte, 4*1024)
		var writtenDev int64
		flashStart := time.Now()

		for {
			n, readErr := srcFile.Read(flashBuf)
			if n > 0 {
				wn, werr := devFile.Write(flashBuf[:n])
				writtenDev += int64(wn)
				now := time.Now()
				elapsed := now.Sub(flashStart).Seconds()
				speed := float64(0)
				if elapsed > 0 {
					speed = float64(writtenDev) / elapsed
				}
				percent := float64(0)
				if written > 0 {
					percent = float64(writtenDev) / float64(written) * 100
				}
				eta := float64(0)
				if speed > 0 && written > 0 {
					eta = float64(written-writtenDev) / speed
				}
				updateProgress.mu.Lock()
				updateProgress.Phase = "flashing"
				updateProgress.Percent = percent
				updateProgress.Written = writtenDev
				updateProgress.Total = written
				updateProgress.Speed = speed
				updateProgress.Eta = eta
				updateProgress.mu.Unlock()
				if werr != nil {
					devFile.Close()
					updateProgress.mu.Lock()
					updateProgress.Error = "写入设备失败: " + werr.Error()
					updateProgress.mu.Unlock()
					advLog("error", "写入设备失败: %v", werr)
					return
				}
			}
			if readErr != nil {
				break
			}
		}
		devFile.Close()

		advLog("info", "写入完成: %s (%d 字节)", updateTargetDev, writtenDev)
		os.Remove(tmpPath)

		updateProgress.mu.Lock()
		updateProgress.Done = true
		updateProgress.Success = true
		updateProgress.Message = "更新已写入 SPI Flash，重启设备后生效"
		updateProgress.Percent = 100
		updateProgress.mu.Unlock()
	}()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "文件已上传，正在后台写入 SPI Flash，请勿关闭设备电源",
	})
}

type BackupProgress struct {
	mu       sync.Mutex
	Phase    string  `json:"phase"`
	Percent  float64 `json:"percent"`
	Read     int64   `json:"read"`
	Total    int64   `json:"total"`
	Speed    float64 `json:"speed"`
	Eta      float64 `json:"eta"`
	Done     bool    `json:"done"`
	Error    string  `json:"error"`
	Success  bool    `json:"success"`
	Running  bool    `json:"running"`
	cancelCh chan struct{}
}

var backupProgress BackupProgress

type BackupDisk struct {
	Device string `json:"device"`
	Model  string `json:"model"`
	Size   int64  `json:"size"`
	Type   string `json:"type"`
}

func handleBackupDisks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	out, err := exec.Command("lsblk", "-b", "-d", "-n", "-o", "NAME,SIZE,MODEL,ROTA,TRAN").Output()
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "lsblk 执行失败: " + err.Error()})
		return
	}

	var disks []BackupDisk
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		name := fields[0]
		if strings.HasPrefix(name, "sr") || strings.HasPrefix(name, "loop") || strings.HasPrefix(name, "zram") {
			continue
		}

		sizeBytes, _ := strconv.ParseInt(fields[1], 10, 64)
		rota := fields[2]
		transport := ""
		if len(fields) >= 5 {
			transport = fields[4]
		}
		model := ""
		if len(fields) >= 4 {
			if len(fields) >= 5 {
				model = strings.Join(fields[3:], " ")
			} else {
				model = strings.Join(fields[3:], " ")
			}
		}

		devType := "HDD"
		if rota == "0" {
			devType = "SSD"
		}
		if transport == "usb" {
			devType = "USB"
		}
		if transport == "mmc" {
			devType = "eMMC"
		}
		if strings.HasPrefix(name, "mtd") || strings.HasPrefix(name, "mmcblk") {
			devType = "eMMC"
		}
		if strings.HasPrefix(name, "nvme") {
			devType = "NVMe"
		}

		disks = append(disks, BackupDisk{
			Device: "/dev/" + name,
			Model:  model,
			Size:   sizeBytes,
			Type:   devType,
		})
	}

	json.NewEncoder(w).Encode(map[string]interface{}{"disks": disks})
}

func handleBackupStream(w http.ResponseWriter, r *http.Request) {
	disk := r.URL.Query().Get("disk")
	if disk == "" {
		http.Error(w, "缺少 disk 参数", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(disk, "/dev/") {
		http.Error(w, "非法设备路径", http.StatusBadRequest)
		return
	}

	backupProgress.mu.Lock()
	if backupProgress.Running {
		backupProgress.mu.Unlock()
		http.Error(w, "已有备份任务在执行", http.StatusConflict)
		return
	}
	backupProgress.Running = true
	backupProgress.Done = false
	backupProgress.Error = ""
	backupProgress.Phase = "reading"
	backupProgress.Percent = 0
	backupProgress.Read = 0
	backupProgress.Total = 0
	backupProgress.Speed = 0
	backupProgress.Eta = 0
	backupProgress.cancelCh = make(chan struct{})
	backupProgress.mu.Unlock()

	devFile, err := os.Open(disk)
	if err != nil {
		backupProgress.mu.Lock()
		backupProgress.Error = "打开设备失败: " + err.Error()
		backupProgress.Running = false
		backupProgress.mu.Unlock()
		http.Error(w, "打开设备失败: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer devFile.Close()

	fi, err := devFile.Stat()
	var totalSize int64
	if err == nil && fi.Size() > 0 {
		totalSize = fi.Size()
	} else {
		out, err2 := exec.Command("blockdev", "--getsize64", disk).Output()
		if err2 == nil {
			totalSize, _ = strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		}
	}

	backupProgress.mu.Lock()
	backupProgress.Total = totalSize
	backupProgress.mu.Unlock()

	fileName := filepath.Base(disk) + ".img.gz"
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+fileName+"\"")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	advLog("info", "开始备份磁盘: %s (大小: %s)", disk, formatBytes(totalSize))

	flusher, canFlush := w.(http.Flusher)

	gw, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
	if err != nil {
		gw = gzip.NewWriter(w)
	}

	buf := make([]byte, 256*1024)
	var totalRead int64
	startTime := time.Now()
	lastReport := startTime

	for {
		select {
		case <-backupProgress.cancelCh:
			gw.Close()
			devFile.Close()
			backupProgress.mu.Lock()
			backupProgress.Error = "备份已取消"
			backupProgress.Running = false
			backupProgress.mu.Unlock()
			advLog("info", "备份已取消: %s", disk)
			return
		default:
		}

		n, readErr := devFile.Read(buf)
		if n > 0 {
			_, werr := gw.Write(buf[:n])
			if werr != nil {
				gw.Close()
				backupProgress.mu.Lock()
				backupProgress.Error = "写入流失败（客户端可能已断开）: " + werr.Error()
				backupProgress.Running = false
				backupProgress.mu.Unlock()
				advLog("error", "备份写入流失败: %v", werr)
				return
			}
			totalRead += int64(n)
			now := time.Now()
			if now.Sub(lastReport) >= 500*time.Millisecond || (readErr != nil && totalRead > 0) {
				elapsed := now.Sub(startTime).Seconds()
				speed := float64(0)
				if elapsed > 0 {
					speed = float64(totalRead) / elapsed
				}
				percent := float64(0)
				if totalSize > 0 {
					percent = float64(totalRead) / float64(totalSize) * 100
				}
				eta := float64(0)
				if speed > 0 && totalSize > 0 {
					eta = float64(totalSize-totalRead) / speed
				}
				backupProgress.mu.Lock()
				backupProgress.Phase = "reading"
				backupProgress.Percent = percent
				backupProgress.Read = totalRead
				backupProgress.Speed = speed
				backupProgress.Eta = eta
				backupProgress.mu.Unlock()
				lastReport = now
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			gw.Close()
			backupProgress.mu.Lock()
			backupProgress.Error = "读取设备失败: " + readErr.Error()
			backupProgress.Running = false
			backupProgress.mu.Unlock()
			return
		}
	}

	if totalSize > 0 && totalRead < totalSize {
		remaining := totalSize - totalRead
		advLog("info", "设备提前返回 EOF，已读 %s，剩余 %s 用零填充", formatBytes(totalRead), formatBytes(remaining))
		zeroBuf := make([]byte, 256*1024)
		for remaining > 0 {
			select {
			case <-backupProgress.cancelCh:
				gw.Close()
				devFile.Close()
				backupProgress.mu.Lock()
				backupProgress.Error = "备份已取消"
				backupProgress.Running = false
				backupProgress.mu.Unlock()
				return
			default:
			}
			chunk := int64(len(zeroBuf))
			if chunk > remaining {
				chunk = remaining
			}
			_, werr := gw.Write(zeroBuf[:chunk])
			if werr != nil {
				gw.Close()
				backupProgress.mu.Lock()
				backupProgress.Error = "写入流失败: " + werr.Error()
				backupProgress.Running = false
				backupProgress.mu.Unlock()
				return
			}
			totalRead += chunk
			remaining -= chunk
			now := time.Now()
			if now.Sub(lastReport) >= 500*time.Millisecond {
				elapsed := now.Sub(startTime).Seconds()
				speed := float64(0)
				if elapsed > 0 {
					speed = float64(totalRead) / elapsed
				}
				percent := float64(totalRead) / float64(totalSize) * 100
				backupProgress.mu.Lock()
				backupProgress.Percent = percent
				backupProgress.Read = totalRead
				backupProgress.Speed = speed
				backupProgress.Eta = float64(totalSize-totalRead) / speed
				backupProgress.mu.Unlock()
				lastReport = now
			}
			if canFlush {
				flusher.Flush()
			}
		}
	}

	gw.Close()
	if canFlush {
		flusher.Flush()
	}

	backupProgress.mu.Lock()
	backupProgress.Done = true
	backupProgress.Running = false
	backupProgress.Percent = 100
	backupProgress.Read = totalRead
	backupProgress.mu.Unlock()

	advLog("info", "备份完成: %s (%s)", disk, formatBytes(totalRead))
}

func handleBackupProgress(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, _ := w.(http.Flusher)
	for {
		backupProgress.mu.Lock()
		data := map[string]interface{}{
			"phase":   backupProgress.Phase,
			"percent": backupProgress.Percent,
			"read":    backupProgress.Read,
			"total":   backupProgress.Total,
			"speed":   backupProgress.Speed,
			"eta":     backupProgress.Eta,
			"running": backupProgress.Running,
			"done":    backupProgress.Done,
			"success": backupProgress.Success,
			"error":   backupProgress.Error,
		}
		done := backupProgress.Done || backupProgress.Error != ""
		running := backupProgress.Running
		backupProgress.mu.Unlock()

		jsonData, _ := json.Marshal(data)
		fmt.Fprintf(w, "event: progress\ndata: %s\n\n", jsonData)
		flusher.Flush()

		if done || !running {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func handleBackupCancel(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	backupProgress.mu.Lock()
	if backupProgress.Running && backupProgress.cancelCh != nil {
		close(backupProgress.cancelCh)
		backupProgress.cancelCh = nil
	}
	backupProgress.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
}

func handleRestoreStream(w http.ResponseWriter, r *http.Request) {
	disk := r.URL.Query().Get("disk")
	if disk == "" {
		http.Error(w, "缺少 disk 参数", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(disk, "/dev/") {
		http.Error(w, "非法设备路径", http.StatusBadRequest)
		return
	}

	backupProgress.mu.Lock()
	if backupProgress.Running {
		backupProgress.mu.Unlock()
		http.Error(w, "已有任务在执行", http.StatusConflict)
		return
	}
	backupProgress.Running = true
	backupProgress.Done = false
	backupProgress.Error = ""
	backupProgress.Phase = "restoring"
	backupProgress.Percent = 0
	backupProgress.Read = 0
	backupProgress.Total = 0
	backupProgress.Speed = 0
	backupProgress.Eta = 0
	backupProgress.cancelCh = make(chan struct{})
	backupProgress.mu.Unlock()

	defer func() {
		r.Body.Close()
		backupProgress.mu.Lock()
		backupProgress.Running = false
		backupProgress.mu.Unlock()
	}()

	contentLen := r.ContentLength
	backupProgress.mu.Lock()
	backupProgress.Total = contentLen
	backupProgress.mu.Unlock()

	gr, err := gzip.NewReader(r.Body)
	if err != nil {
		backupProgress.mu.Lock()
		backupProgress.Error = "解压失败: " + err.Error()
		backupProgress.mu.Unlock()
		http.Error(w, "解压失败: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer gr.Close()

	devFile, err := os.OpenFile(disk, os.O_WRONLY|os.O_SYNC, 0)
	if err != nil {
		backupProgress.mu.Lock()
		backupProgress.Error = "打开设备失败: " + err.Error()
		backupProgress.mu.Unlock()
		http.Error(w, "打开设备失败: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer devFile.Close()

	advLog("info", "开始还原磁盘: %s", disk)

	buf := make([]byte, 256*1024)
	var totalWritten int64
	startTime := time.Now()
	lastReport := startTime

	for {
		select {
		case <-backupProgress.cancelCh:
			devFile.Close()
			gr.Close()
			backupProgress.mu.Lock()
			backupProgress.Error = "还原已取消，设备可能无法使用"
			backupProgress.Running = false
			backupProgress.mu.Unlock()
			advLog("warning", "还原已取消: %s", disk)
			return
		default:
		}

		n, readErr := gr.Read(buf)
		if n > 0 {
			wn, werr := devFile.Write(buf[:n])
			totalWritten += int64(wn)
			now := time.Now()
			if now.Sub(lastReport) >= 500*time.Millisecond || (readErr != nil && totalWritten > 0) {
				elapsed := now.Sub(startTime).Seconds()
				speed := float64(0)
				if elapsed > 0 {
					speed = float64(totalWritten) / elapsed
				}
				percent := float64(0)
				if contentLen > 0 {
					percent = float64(totalWritten) / float64(contentLen) * 100
				}
				eta := float64(0)
				if speed > 0 && contentLen > 0 {
					remaining := contentLen - totalWritten
					if remaining > 0 {
						eta = float64(remaining) / speed
					}
				}
				backupProgress.mu.Lock()
				backupProgress.Phase = "restoring"
				backupProgress.Percent = percent
				backupProgress.Read = totalWritten
				backupProgress.Speed = speed
				backupProgress.Eta = eta
				backupProgress.mu.Unlock()
				lastReport = now
			}
			if werr != nil {
				backupProgress.mu.Lock()
				backupProgress.Error = "写入设备失败: " + werr.Error()
				backupProgress.mu.Unlock()
				advLog("error", "还原写入失败: %v", werr)
				return
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			backupProgress.mu.Lock()
			backupProgress.Error = "读取上传数据失败: " + readErr.Error()
			backupProgress.mu.Unlock()
			return
		}
	}

	devFile.Close()

	backupProgress.mu.Lock()
	backupProgress.Done = true
	backupProgress.Success = true
	backupProgress.Percent = 100
	backupProgress.Read = totalWritten
	backupProgress.mu.Unlock()

	advLog("info", "还原完成: %s (%s)", disk, formatBytes(totalWritten))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "还原完成，重启设备后生效",
	})
}

const fwChainInput = "RKDEV_INPUT"
const fwChainForward = "RKDEV_FORWARD"
const fwChainNat = "RKDEV_NAT"

type FwRule struct {
	ID        int    `json:"id"`
	Chain     string `json:"chain"`
	Action    string `json:"action"`
	Protocol  string `json:"protocol"`
	SrcIP     string `json:"srcIP"`
	SrcPort   string `json:"srcPort"`
	DstIP     string `json:"dstIP"`
	DstPort   string `json:"dstPort"`
	IFace     string `json:"iface"`
	NatTarget string `json:"natTarget"`
	Comment   string `json:"comment"`
	Raw       string `json:"raw"`
}

type FwStatus struct {
	Enabled    bool      `json:"enabled"`
	InputPolicy string   `json:"inputPolicy"`
	ForwardPolicy string  `json:"forwardPolicy"`
	InputRules []FwRule  `json:"inputRules"`
	ForwardRules []FwRule `json:"forwardRules"`
	NatRules   []FwRule  `json:"natRules"`
	Error      string    `json:"error,omitempty"`
}

func runIptables(args ...string) (string, error) {
	fullArgs := append([]string{}, args...)
	out, err := exec.Command("iptables", fullArgs...).CombinedOutput()
	return string(out), err
}

func runIptablesT(table string, args ...string) (string, error) {
	fullArgs := append([]string{"-t", table}, args...)
	out, err := exec.Command("iptables", fullArgs...).CombinedOutput()
	return string(out), err
}

func ensureFwChains() {
	runIptables("-N", fwChainInput, "2>/dev/null")
	runIptables("-N", fwChainForward, "2>/dev/null")
	runIptables("-t", "nat", "-N", fwChainNat, "2>/dev/null")

	out, _ := exec.Command("iptables", "-C", "INPUT", "-j", fwChainInput).CombinedOutput()
	if len(out) > 0 {
		runIptables("-I", "INPUT", "1", "-j", fwChainInput)
	}
	out, _ = exec.Command("iptables", "-C", "FORWARD", "-j", fwChainForward).CombinedOutput()
	if len(out) > 0 {
		runIptables("-I", "FORWARD", "1", "-j", fwChainForward)
	}
	out, _ = exec.Command("iptables", "-t", "nat", "-C", "PREROUTING", "-j", fwChainNat).CombinedOutput()
	if len(out) > 0 {
		runIptables("-t", "nat", "-I", "PREROUTING", "1", "-j", fwChainNat)
	}
}

func handleFirewallStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ensureFwChains()

	status := FwStatus{}

	out, err := exec.Command("iptables", "-S", "INPUT").Output()
	if err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			if strings.Contains(line, "policy DROP") || strings.Contains(line, "-P INPUT DROP") {
				status.InputPolicy = "DROP"
			} else if strings.Contains(line, "policy ACCEPT") || strings.Contains(line, "-P INPUT ACCEPT") {
				status.InputPolicy = "ACCEPT"
			}
		}
	}

	out, err = exec.Command("iptables", "-S", "FORWARD").Output()
	if err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			if strings.Contains(line, "policy DROP") || strings.Contains(line, "-P FORWARD DROP") {
				status.ForwardPolicy = "DROP"
			} else if strings.Contains(line, "policy ACCEPT") || strings.Contains(line, "-P FORWARD ACCEPT") {
				status.ForwardPolicy = "ACCEPT"
			}
		}
	}

	_, _, inputRules := parseFwChain(fwChainInput, false)
	_, _, forwardRules := parseFwChain(fwChainForward, false)
	_, _, natRules := parseFwChain(fwChainNat, true)

	status.InputRules = inputRules
	status.ForwardRules = forwardRules
	status.NatRules = natRules
	status.Enabled = len(inputRules) > 0 || len(forwardRules) > 0 || len(natRules) > 0

	json.NewEncoder(w).Encode(status)
}

func parseFwChain(chain string, nat bool) (string, error, []FwRule) {
	var rules []FwRule
	var tableArg string
	if nat {
		tableArg = "-t nat"
	}

	out, err := exec.Command("sh", "-c", "iptables "+tableArg+" -L "+chain+" -n --line-numbers 2>/dev/null").Output()
	if err != nil {
		return "", err, nil
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Chain") || strings.HasPrefix(line, "num") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		id, _ := strconv.Atoi(fields[0])
		rule := FwRule{ID: id, Raw: line}

		if len(fields) > 1 {
			rule.Action = fields[1]
		}
		for i := 2; i < len(fields); i++ {
			if fields[i] == "tcp" || fields[i] == "udp" || fields[i] == "icmp" || fields[i] == "all" {
				rule.Protocol = fields[i]
			}
			if fields[i] == "anywhere" || strings.Contains(fields[i], ".") {
				if rule.SrcIP == "" {
					rule.SrcIP = fields[i]
				} else if rule.DstIP == "" {
					rule.DstIP = fields[i]
				}
			}
			if strings.Contains(fields[i], "dpt:") {
				rule.DstPort = strings.TrimPrefix(fields[i], "dpt:")
			}
			if strings.Contains(fields[i], "spt:") {
				rule.SrcPort = strings.TrimPrefix(fields[i], "spt:")
			}
			if strings.Contains(fields[i], "to:") {
				rule.NatTarget = strings.TrimPrefix(fields[i], "to:")
			}
		}

		if nat {
			rule.Chain = "nat"
		} else if chain == fwChainInput {
			rule.Chain = "input"
		} else {
			rule.Chain = "forward"
		}
		rules = append(rules, rule)
	}
	return "", nil, rules
}

func handleFirewallApply(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}

	var req struct {
		Chain    string `json:"chain"`
		Action   string `json:"action"`
		Protocol string `json:"protocol"`
		SrcIP    string `json:"srcIP"`
		DstIP    string `json:"dstIP"`
		DstPort  string `json:"dstPort"`
		NatTarget string `json:"natTarget"`
		Comment  string `json:"comment"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "参数解析失败: " + err.Error()})
		return
	}

	ensureFwChains()

	var args []string
	var table string
	var chain string

	if req.Chain == "nat" {
		table = "nat"
		chain = fwChainNat
		if req.Action == "dnat" {
			args = []string{"-t", "nat", "-A", chain, "-p", req.Protocol}
			if req.DstPort != "" {
				args = append(args, "--dport", req.DstPort)
			}
			args = append(args, "-j", "DNAT", "--to-destination", req.NatTarget)
		} else if req.Action == "snat" {
			args = append(args, "-j", "SNAT", "--to-source", req.NatTarget)
		} else if req.Action == "masquerade" {
			args = []string{"-t", "nat", "-A", chain, "-j", "MASQUERADE"}
		}
	} else {
		if req.Chain == "input" {
			chain = fwChainInput
		} else {
			chain = fwChainForward
		}
		args = []string{"-A", chain, "-j", req.Action}
		if req.Protocol != "" && req.Protocol != "all" {
			args = []string{"-A", chain, "-p", req.Protocol, "-j", req.Action}
		}
		if req.SrcIP != "" {
			args = append(args, "-s", req.SrcIP)
		}
		if req.DstIP != "" {
			args = append(args, "-d", req.DstIP)
		}
		if req.DstPort != "" {
			args = append(args, "--dport", req.DstPort)
		}
	}

	if req.Comment != "" {
		args = append(args, "-m", "comment", "--comment", req.Comment)
	}

	if table == "nat" {
		out, err := exec.Command("iptables", args...).CombinedOutput()
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "添加规则失败: " + string(out)})
			return
		}
	} else {
		out, err := exec.Command("iptables", args...).CombinedOutput()
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"error": "添加规则失败: " + string(out)})
			return
		}
	}

	advLog("info", "防火墙添加规则: %s %v", chain, args)
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func handleFirewallDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}

	var req struct {
		Chain string `json:"chain"`
		ID    int    `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "参数解析失败: " + err.Error()})
		return
	}

	var chain string
	var table string
	if req.Chain == "input" {
		chain = fwChainInput
	} else if req.Chain == "forward" {
		chain = fwChainForward
	} else if req.Chain == "nat" {
		chain = fwChainNat
		table = "nat"
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "未知链"})
		return
	}

	var out string
	var err error
	if table == "nat" {
		out, err = runIptablesT("nat", "-D", chain, strconv.Itoa(req.ID))
	} else {
		out, err = runIptables("-D", chain, strconv.Itoa(req.ID))
	}
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "删除规则失败: " + out})
		return
	}

	advLog("info", "防火墙删除规则: %s #%d", chain, req.ID)
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func handleFirewallToggle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}

	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "参数解析失败: " + err.Error()})
		return
	}

	ensureFwChains()

	if req.Enabled {
		out, _ := exec.Command("iptables", "-C", "INPUT", "-j", fwChainInput).CombinedOutput()
		if len(out) > 0 {
			runIptables("-I", "INPUT", "1", "-j", fwChainInput)
		}
		out, _ = exec.Command("iptables", "-C", "FORWARD", "-j", fwChainForward).CombinedOutput()
		if len(out) > 0 {
			runIptables("-I", "FORWARD", "1", "-j", fwChainForward)
		}
		out, _ = exec.Command("iptables", "-t", "nat", "-C", "PREROUTING", "-j", fwChainNat).CombinedOutput()
		if len(out) > 0 {
			runIptables("-t", "nat", "-I", "PREROUTING", "1", "-j", fwChainNat)
		}
		advLog("info", "防火墙已启用")
	} else {
		for {
			out, err := exec.Command("iptables", "-D", "INPUT", "-j", fwChainInput).CombinedOutput()
			if err != nil || len(out) == 0 {
				break
			}
		}
		for {
			out, err := exec.Command("iptables", "-D", "FORWARD", "-j", fwChainForward).CombinedOutput()
			if err != nil || len(out) == 0 {
				break
			}
		}
		for {
			out, err := exec.Command("iptables", "-t", "nat", "-D", "PREROUTING", "-j", fwChainNat).CombinedOutput()
			if err != nil || len(out) == 0 {
				break
			}
		}
		advLog("info", "防火墙已禁用")
	}

	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

