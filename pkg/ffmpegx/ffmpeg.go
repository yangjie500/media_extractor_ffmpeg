package ffmpegx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type StreamInfo struct {
	HasVideo bool
	HasAudio bool
	Format   string
	Duration float64
}

func ffmpegPath() string {
	if p := os.Getenv("FFMPEG_BIN"); p != "" {
		return p
	}
	return "ffmpeg"
}
func ffprobePath() string {
	if p := os.Getenv("FFPROBE_BIN"); p != "" {
		return p
	}
	return "ffprobe"
}

func EnsureBinariesExists() error {
	if _, err := exec.LookPath(ffmpegPath()); err != nil {
		return fmt.Errorf("ffmpeg not found: %w", err)
	}

	if _, err := exec.LookPath(ffprobePath()); err != nil {
		return fmt.Errorf("ffprobe not found: %w", err)
	}

	return nil
}

func Probe(ctx context.Context, path string) (StreamInfo, error) {
	type ffprobeOut struct {
		Streams []struct {
			CodecType string `json:"codec_type"`
		} `json:"streams"`
		Format struct {
			FormatName string `json:"format_name"`
			Duration   string `json:"duration"`
		} `json:"format"`
	}

	var si StreamInfo
	if _, err := os.Stat(path); err != nil {
		return si, fmt.Errorf("probe: stat %q; %w", path, err)
	}

	args := []string{
		"-v", "error",
		"-print_format", "json",
		"-show_streams",
		"-show_format",
		path,
	}

	stdout, stderr, err := run(ctx, ffprobePath(), args...)
	if err != nil {
		return si, fmt.Errorf("ffprobe failed: %v, stderr=%s", err, tail(stderr, 8<<10))
	}

	var out ffprobeOut
	if jerr := json.Unmarshal(stdout, &out); jerr != nil {
		return si, fmt.Errorf("ffprobe json parse %w", jerr)
	}

	si.Format = out.Format.FormatName
	for _, s := range out.Streams {
		switch s.CodecType {
		case "video":
			si.HasVideo = true
		case "audio":
			si.HasAudio = true
		}
	}

	if d := strings.TrimSpace(out.Format.Duration); d != "" {
		fmt.Sscanf(d, "%f", &si.Duration)
	}

	return si, nil
}

func MergeAV(ctx context.Context, videoPath, audioPath, outPath string, audioCodec string) error {
	if err := EnsureBinariesExists(); err != nil {
		return err
	}

	// Basic Validation
	if err := mustReadable(videoPath); err != nil {
		return fmt.Errorf("video %w", err)
	}
	if err := mustReadable(audioPath); err != nil {
		return fmt.Errorf("audio %w", err)
	}

	vInfo, err := Probe(ctx, videoPath)
	if err != nil {
		return err
	}
	aInfo, err := Probe(ctx, audioPath)
	if err != nil {
		return err
	}

	if !vInfo.HasVideo {
		return errors.New("input video has no video stream")
	}
	if !aInfo.HasAudio {
		return errors.New("input audio has no audio stream")
	}

	// atomic outpupt
	tmpDir := filepath.Dir(outPath)
	tmpFile := filepath.Join(tmpDir, "."+filepath.Base(outPath)+".tmp")
	_ = os.Remove(tmpFile)

	aCodec := "aac"
	if strings.TrimSpace(strings.ToLower(audioCodec)) != "" {
		aCodec = audioCodec
	}

	// ffmpeg command:
	// ffmpeg -v error -nostdin -y -i video -i audio \
	//   -map 0:v:0 -map 1:a:0 -c:v copy -c:a <codec> -shortest out
	args := []string{
		"-v", "error",
		"-nostdin",
		"-y",
		"-i", videoPath,
		"-i", audioPath,
		"-map", "0:v:0",
		"-map", "1:a:0",
		"-c:v", "copy",
		"-f", "mp4",
		"-c:a", aCodec,
		"-shortest",
		tmpFile,
	}

	_, stderr, runErr := run(ctx, ffmpegPath(), args...)
	if runErr != nil {
		return fmt.Errorf("ffmpeg merge failed: %v, stderr=%s", runErr, tail(stderr, 16<<10))
	}

	if err := os.Rename(tmpFile, outPath); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("rename output: %w", err)
	}

	return nil
}

// ----- Helper -----
func run(ctx context.Context, bin string, args ...string) ([]byte, []byte, error) {
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func tail(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[len(b)-max:])
}

func mustReadable(p string) error {
	st, err := os.Stat(p)
	if err != nil {
		return err
	}
	if st.IsDir() {
		return fmt.Errorf("%w: path is a directory", fs.ErrInvalid)
	}
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	f.Close()
	return nil
}
