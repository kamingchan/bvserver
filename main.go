package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"golang.org/x/sync/errgroup"
)

const (
	ChunkSize = bytefmt.MEGABYTE * 10
	CDN       = "upos-sz-mirroraliov.bilivideo.com"
)

var (
	client = &http.Client{
		Transport: &http.Transport{
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
	done   atomic.Bool
	err    error
	file   string
	header http.Header
}

func (t *VideoTask) Done() bool {
	return t.done.Load()
}

func (t *VideoTask) Error() error {
	if t.err != nil {
		return t.err
	}
	_, err := os.Stat(t.file)
	return err
}

func NewVideoTask(key string, link url.URL, header http.Header) (task *VideoTask) {
	task = new(VideoTask)

	size, header, err := GetVideoSize(link.String(), header)
	if err != nil {
		slog.Warn("get video size failed", slog.String("key", key), slog.Any("error", err))
		task.err = err
		return
	}
	slog.Info("get video size", slog.String("key", key), slog.Int64("size", size))
	task.header = header.Clone()

	tempF, err := os.CreateTemp("", key)
	if err != nil {
		slog.Warn("create temp file failed", slog.String("key", key), slog.Any("error", err))
		task.err = err
		return
	}
	task.file = tempF.Name()
	go func() {
		start := time.Now()
		err = DownloadChunk(link, header, size, tempF)
		tempF.Close()
		if err != nil {
			os.Remove(tempF.Name())
			task.err = err
		} else {
			task.done.Store(true)
		}
		pass := time.Since(start).Seconds()
		speed := float64(size) / pass / bytefmt.MEGABYTE
		slog.Info("download finished", slog.String("key", key), slog.String("path", tempF.Name()), slog.Float64("speed(MB/s)", speed))
	}()

	return task
}

func ResponseWithFile(writer http.ResponseWriter, request *http.Request, task *VideoTask) {
	copyHeaders(writer.Header(), task.header)
	http.ServeFile(writer, request, task.file)
}

func ResponseByPass(writer http.ResponseWriter, link url.URL, header http.Header) {
	req, err := http.NewRequest(http.MethodGet, link.String(), nil)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	req.Header = header.Clone()

	resp, err := client.Do(req)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	copyHeaders(writer.Header(), resp.Header)
	writer.WriteHeader(resp.StatusCode)
	io.Copy(writer, resp.Body)
}

func copyHeaders(dst http.Header, src http.Header) {
	for key := range src {
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

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	resp.Body.Close()

	if resp.StatusCode >= 300 {
		return 0, nil, errors.New("not 2xx response")
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

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return errors.New("not partial content")
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
	eg, _ := errgroup.WithContext(context.Background())
	for i := 0; i < int(size); i += ChunkSize {
		var (
			start = i
			end   = i + ChunkSize - 1
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

func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	key := request.URL.Path
	key = strings.ReplaceAll(key, "/", "")

	var (
		link   = *request.URL
		header = request.Header
	)
	link.Scheme = "http"
	link.Host = CDN

	mu.Lock()

	c, ok := cache[key]
	if ok && c.Done() {
		// hit cache
	} else if ok && c.Error() == nil {
		// still downloading
		c = nil
	} else if !ok || c.Error() != nil {
		delete(cache, key)
		c = nil

		cache[key] = NewVideoTask(key, link, header)
	} else {
		// other cases
	}

	mu.Unlock()

	if c != nil {
		ResponseWithFile(writer, request, c)
	} else {
		ResponseByPass(writer, link, header)
	}
}

func main() {
	http.ListenAndServe(":2333", &handler{})
}
