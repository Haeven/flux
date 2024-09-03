package encoder

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const segmentDuration = 5 // Segment duration in seconds

// GenerateSegments generates video segments encoded with VP9 using Av1an.
func GenerateSegments(videoFile, outputDir string) error {
	baseName := filepath.Base(videoFile)

	// Make sure the output directory exists
	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating output directory: %w", err)
	}

	// Define resolutions and bitrates for encoding
	resolutions := []string{"144p", "240p", "720p", "1080p", "1440p", "2160p"}

	for _, resolution := range resolutions {
		bitrate := CalculateVP9Bitrate(resolution)
		segmentPattern := filepath.Join(outputDir, fmt.Sprintf("%s_%s_segment_%%03d.webm", baseName, resolution))

		cmd := exec.Command("av1an", "-i", videoFile, "-o", outputDir, "-c", "vp9", "-b", bitrate, "-p", segmentPattern)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("error encoding video segments: %w", err)
		}
	}

	return nil
}

// CalculateVP9Bitrate calculates the ideal bitrate for VP9 encoding based on resolution.
func CalculateVP9Bitrate(resolution string) string {
	switch resolution {
	case "144p":
		return "150k" // 150 kbps
	case "240p":
		return "300k" // 300 kbps
	case "720p":
		return "1500k" // 1.5 Mbps
	case "1080p":
		return "3000k" // 3 Mbps
	case "1440p":
		return "6000k" // 6 Mbps
	case "2160p":
		return "12000k" // 12 Mbps
	default:
		return "1500k" // Default bitrate
	}
}