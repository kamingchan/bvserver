package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/rs/cors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

var (
	Listen     string
	ChunkSize  int
	CDN        string
	ServerName string
)

func init() {
	flag.StringVar(&Listen, "listen", "127.0.0.1:2333", "listen address")
	flag.IntVar(&ChunkSize, "chunksize", 10, "chunk size in MB")
	flag.StringVar(&CDN, "cdn", "upos-sz-mirrorali.bilivideo.com", "Bilibili CDN host")
	flag.StringVar(&ServerName, "server", "bvserver", "Custom server name")
}

var (
	dialer = &net.Dialer{
		Timeout:   time.Second * 5,
		KeepAlive: time.Second * 15,
	}
	h1Client = &http.Client{
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			Proxy:                 http.ProxyFromEnvironment,
			ForceAttemptHTTP2:     false,
			MaxIdleConns:          10,
			MaxIdleConnsPerHost:   10,
			MaxConnsPerHost:       200,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	h2Client = &http.Client{
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			Proxy:                 http.ProxyFromEnvironment,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          10,
			MaxIdleConnsPerHost:   10,
			MaxConnsPerHost:       200,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
)

type VideoTask struct {
	done   chan struct{}
	err    error
	key    string
	file   string
	header http.Header

	hit   time.Time
	size  int64
	speed float64
}

func (t *VideoTask) MarkDone() {
	close(t.done)
}

func (t *VideoTask) Done() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

func (t *VideoTask) Wait() <-chan struct{} {
	return t.done
}

func (t *VideoTask) Error() error {
	if t.err != nil {
		return t.err
	}
	if t.Done() == false {
		return nil
	}
	_, err := os.Stat(t.file)
	return err
}

func (t *VideoTask) Hit() {
	t.hit = time.Now()
}

func NewVideoTask(key string, link url.URL, header http.Header) (task *VideoTask) {
	task = new(VideoTask)
	task.key = key
	task.done = make(chan struct{})
	task.hit = time.Now()

	size, h, err := GetVideoSize(link.String(), header)
	if err != nil {
		slog.Warn("get video size failed", slog.String("key", key), slog.Any("error", err))
		task.err = err
		return
	}
	slog.Info("get video size", slog.String("key", key), slog.String("size", bytefmt.ByteSize(uint64(size))))

	// copy header
	task.header = h.Clone()
	task.header.Del("Via")
	task.header.Del("X-Cache")
	task.header.Set("X-Server", ServerName)
	task.file = path.Join(os.TempDir(), key)

	if info, err := os.Stat(task.file); err == nil && info.Size() == size {
		task.MarkDone()
		slog.Info("file exists", slog.String("key", key), slog.String("path", task.file))
		task.size = size
		return
	}

	tempF, err := os.CreateTemp("", key)
	if err != nil {
		slog.Warn("create temp file failed", slog.String("key", key), slog.Any("error", err))
		task.err = err
		return
	}

	closeAndClean := func() {
		tempF.Close()
		os.Remove(tempF.Name())
	}

	// preallocate
	_, err = tempF.WriteAt([]byte{0}, size-1)
	if err != nil {
		slog.Warn("preallocate failed", slog.String("key", key), slog.Any("error", err))
		task.err = err
		closeAndClean()
		return
	}
	err = tempF.Sync()
	if err != nil {
		slog.Warn("sync failed", slog.String("key", key), slog.Any("error", err))
		task.err = err
		closeAndClean()
		return
	}
	go func() {
		start := time.Now()
		err = DownloadChunk(link, header, size, tempF)
		tempF.Close()
		if err != nil {
			slog.Warn("download failed", slog.String("key", key), slog.Any("error", err))
			task.err = err
			closeAndClean()
			return
		}

		err = os.Rename(tempF.Name(), task.file)
		if err != nil {
			slog.Warn("rename temp file failed", slog.String("key", key), slog.Any("error", err))
			task.err = err
			closeAndClean()
			return
		}

		task.MarkDone()
		pass := time.Since(start).Seconds()
		speed := float64(size) / pass
		slog.Info("download finished",
			slog.String("key", key),
			slog.String("path", tempF.Name()),
			slog.String("speed", bytefmt.ByteSize(uint64(speed))+"/s"),
		)
		task.size = size
		task.speed = speed
	}()

	return task
}

var (
	CacheBytes  = &atomic.Uint64{}
	ByPassBytes = &atomic.Uint64{}
)

type wrapper struct {
	http.ResponseWriter

	written atomic.Uint64
}

func (w *wrapper) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	CacheBytes.Add(uint64(n))
	w.written.Add(uint64(n))
	return n, err
}

func ResponseWithFile(_writer http.ResponseWriter, request *http.Request, task *VideoTask) uint64 {
	copyHeaders(_writer.Header(), task.header)
	writer := &wrapper{ResponseWriter: _writer}
	http.ServeFile(writer, request, task.file)
	return writer.written.Load()
}

func ResponseByPass(writer http.ResponseWriter, link url.URL, header http.Header) int64 {
	req, err := http.NewRequest(http.MethodGet, link.String(), nil)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return 0
	}
	req.Header = header.Clone()

	resp, err := h1Client.Do(req)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return 0
	}
	defer resp.Body.Close()

	copyHeaders(writer.Header(), resp.Header)
	writer.WriteHeader(resp.StatusCode)
	written, _ := io.Copy(writer, resp.Body)
	ByPassBytes.Add(uint64(written))
	return written
}

func HandleStat(writer http.ResponseWriter, request *http.Request) error {
	var (
		c = bytefmt.ByteSize(CacheBytes.Load())
		b = bytefmt.ByteSize(ByPassBytes.Load())
	)

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)

	available, _ := diskAvailable()

	mu.Lock()
	defer mu.Unlock()
	var summaries []any
	for key, t := range cache {
		summaries = append(summaries, map[string]any{
			"key":   key,
			"hit":   t.hit,
			"file":  t.file,
			"size":  bytefmt.ByteSize(uint64(t.size)),
			"speed": bytefmt.ByteSize(uint64(t.speed)) + "/s",
		})
	}

	return json.NewEncoder(writer).Encode(map[string]any{
		"cache_hit": c,
		"bypass":    b,
		"available": available,
		"tasks":     summaries,
	})
}

func copyHeaders(dst http.Header, src http.Header) {
	for key := range src {
		// ignore cors headers
		if strings.HasPrefix(key, "Access-Control-") {
			continue
		}
		values := src.Values(key)
		for _, v := range values {
			dst.Add(key, v)
		}
	}
}

func GetVideoSize(link string, header http.Header) (int64, http.Header, error) {
	req, err := http.NewRequest(http.MethodHead, link, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header = header.Clone()
	req.Header.Del("Range")

	resp, err := h2Client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	resp.Body.Close()

	if resp.StatusCode >= 300 {
		return 0, nil, errors.New("not 2xx response")
	}
	if resp.ContentLength == 0 {
		return 0, nil, fmt.Errorf("invalid content length: %d, status code: %d", resp.ContentLength, resp.StatusCode)
	}
	return resp.ContentLength, resp.Header, nil
}

func DownloadToFile(link url.URL, header http.Header, start, end int, f *os.File) error {
	req, err := http.NewRequest(http.MethodGet, link.String(), nil)
	if err != nil {
		return err
	}
	req.Header = header.Clone()
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := h1Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("not 206 response: %d", resp.StatusCode)
	}
	// write body to file
	var (
		idx = start
		buf = make([]byte, 4*bytefmt.KILOBYTE)
		n   int
	)
	for {
		n, err = resp.Body.Read(buf)
		if n > 0 {
			_, err = f.WriteAt(buf[:n], int64(idx))
			if err != nil {
				return err
			}
			idx += n
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func DownloadChunk(link url.URL, header http.Header, size int64, f *os.File) error {
	chunkSize := ChunkSize * bytefmt.MEGABYTE

	eg, _ := errgroup.WithContext(context.Background())
	for i := 0; i < int(size); i += chunkSize {
		var (
			start = i
			end   = i + chunkSize - 1
		)
		if end >= int(size) {
			end = int(size) - 1
		}
		eg.Go(func() error {
			return DownloadToFile(link, header, start, end, f)
		})
	}
	return eg.Wait()
}

var (
	cache = make(map[string]*VideoTask)
	mu    sync.Mutex
)

type handler struct{}

func ServeHTTP(writer http.ResponseWriter, request *http.Request) error {
	switch {
	case strings.HasPrefix(request.URL.Path, "/stat"):
		return HandleStat(writer, request)
	case strings.HasPrefix(request.URL.Path, "/upgcxcode"):
		return HandleVideo(writer, request)
	default:
		writer.Header().Set("Location", "https://github.com/kamingchan/bvserver")
		writer.WriteHeader(http.StatusTemporaryRedirect)
		return nil
	}
}

func HandleVideo(writer http.ResponseWriter, request *http.Request) error {
	key := request.URL.Path
	key = strings.ReplaceAll(key, "/", "")

	var (
		link   = *request.URL
		header = request.Header
	)
	link.Scheme = "http"
	link.Host = CDN

	var (
		task *VideoTask
		hit  bool
	)

	mu.Lock()

	c, ok := cache[key]
	if ok && c.Done() {
		task = c
		hit = true
		// hit cache
	} else if ok && c.Error() == nil {
		// still downloading
		task = c
		hit = false
	} else if !ok || c.Error() != nil {
		delete(cache, key)
		cache[key] = NewVideoTask(key, link, header)

		task = cache[key]
		hit = false
	}

	mu.Unlock()

	if hit {
		c.Hit()
		written := ResponseWithFile(writer, request, c)
		slog.Info("cache hit", slog.String("key", key), slog.String("size", bytefmt.ByteSize(written)))
	} else {
		done := make(chan struct{})
		go func() {
			start := time.Now()
			slog.Info("bypass started", slog.String("key", key))
			bytes := ResponseByPass(writer, link, header)
			pass := time.Since(start).Seconds()
			slog.Info("bypass finished", slog.String("key", key), slog.String("speed", bytefmt.ByteSize(uint64(float64(bytes)/pass))+"/s"))
			close(done)
		}()

		select {
		case <-done:
			// bypass done, return
			return nil
		case <-task.Wait():
			// task is done, break
			request.Context().Value(ctxConnKey{}).(net.Conn).Close()
			slog.Info("download finished, interrupt bypass", slog.String("key", key))
			<-done
		}
	}
	return nil
}

func diskAvailable() (float64, error) {
	tmpDir := os.TempDir()
	var stat unix.Statfs_t
	err := unix.Statfs(tmpDir, &stat)
	if err != nil {
		return 0, err
	}
	return float64(stat.Bavail) / float64(stat.Blocks), nil
}

func worker() {
	t := time.NewTicker(time.Minute)

	for range t.C {
		av, err := diskAvailable()
		if err != nil {
			continue
		}
		if av > 0.1 {
			continue
		}

		// clean cache
		mu.Lock()
		tasks := lo.Values(cache)
		tasks = lo.Filter(tasks, func(t *VideoTask, _ int) bool { return t.Done() })
		slices.SortFunc(tasks, func(i, j *VideoTask) int { return int(i.hit.Unix() - j.hit.Unix()) })
		// remove 30% of cache
		for i := 0; i < len(tasks)/3; i++ {
			delete(cache, tasks[i].key)
			os.Remove(tasks[i].file)
		}
		mu.Unlock()

		after, _ := diskAvailable()
		slog.Info("disk full, clean cache", slog.String("before", fmt.Sprintf("%.2f", av)), slog.String("after", fmt.Sprintf("%.2f", after)))
	}
}

func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ServeHTTP(writer, request)
}

type ctxConnKey struct{}

func main() {
	flag.Parse()
	s := &http.Server{
		Addr:    Listen,
		Handler: cors.AllowAll().Handler(&handler{}),
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, ctxConnKey{}, c)
		},
	}
	go s.ListenAndServe()
	go worker()
	select {}
}
