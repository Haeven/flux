package codec

import (
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

type SeaweedFSClient struct {
	MasterURL string
	VolumeURL string
}

const segmentDuration = 5 // Segment duration in seconds

type MPD struct {
	XMLName                   xml.Name `xml:"MPD"`
	XMLNs                     string   `xml:"xmlns,attr"`
	XMLNsXsi                  string   `xml:"xmlns:xsi,attr"`
	XsiSchemaLocation         string   `xml:"xsi:schemaLocation,attr"`
	MediaPresentationDuration string   `xml:"mediaPresentationDuration,attr"`
	MinBufferTime             string   `xml:"minBufferTime,attr"`
	Periods                   []Period `xml:"Period"`
}

type Period struct {
	XMLName        xml.Name        `xml:"Period"`
	Duration       string          `xml:"duration,attr"`
	AdaptationSets []AdaptationSet `xml:"AdaptationSet"`
}

type AdaptationSet struct {
	XMLName         xml.Name         `xml:"AdaptationSet"`
	MimeType        string           `xml:"mimeType,attr"`
	Codecs          string           `xml:"codecs,attr"`
	Width           string           `xml:"width,attr"`
	Height          string           `xml:"height,attr"`
	FrameRate       string           `xml:"frameRate,attr"`
	Representations []Representation `xml:"Representation"`
}

type Representation struct {
	XMLName     xml.Name    `xml:"Representation"`
	ID          string      `xml:"id,attr"`
	Bandwidth   string      `xml:"bandwidth,attr"`
	Codecs      string      `xml:"codecs,attr"`
	Width       string      `xml:"width,attr"`
	Height      string      `xml:"height,attr"`
	FrameRate   string      `xml:"frameRate,attr"`
	BaseURL     string      `xml:"BaseURL"`
	SegmentList SegmentList `xml:"SegmentList"`
}

type SegmentList struct {
	XMLName     xml.Name     `xml:"SegmentList"`
	Duration    string       `xml:"duration,attr"`
	Timescale   string       `xml:"timescale,attr"`
	SegmentURLs []SegmentURL `xml:"SegmentURL"`
}

type SegmentURL struct {
	XMLName xml.Name `xml:"SegmentURL"`
	Media   string   `xml:"media,attr"`
}

// generateSegments generates video segments using ffmpeg
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

// calculateVP9Bitrate calculates the ideal bitrate for VP9 encoding based on resolution.
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

// generateMPD generates an MPD file for the video segments
func GenerateMPD(outputDir string) error {
	segmentFiles, err := filepath.Glob(filepath.Join(outputDir, "segment_*.webm"))
	if err != nil {
		return fmt.Errorf("error reading segment files: %w", err)
	}

	var segmentURLs []SegmentURL
	for _, file := range segmentFiles {
		filename := filepath.Base(file)
		segmentURLs = append(segmentURLs, SegmentURL{Media: filename})
	}

	mpd := MPD{
		XMLNs:                     "urn:mpeg:dash:schema:mpd:2011",
		XMLNsXsi:                  "http://www.w3.org/2001/XMLSchema-instance",
		XsiSchemaLocation:         "urn:mpeg:dash:schema:mpd:2011 http://www.mpegdash.org/schemas/2011/MPD.xsd",
		MediaPresentationDuration: "PT" + strconv.Itoa(len(segmentURLs)*10) + "S", // Example duration, adjust as needed
		MinBufferTime:             "PT1.5S",
		Periods: []Period{
			{
				Duration: "PT" + strconv.Itoa(len(segmentURLs)*10) + "S", // Example duration, adjust as needed
				AdaptationSets: []AdaptationSet{
					{
						MimeType: "video/webm",
						Codecs:   "vp09.00.10.08",
						Representations: []Representation{
							CreateRepresentation("144p", "150000", "256", "144", "25", "https://example.com/video_144p/", segmentURLs),
							CreateRepresentation("240p", "300000", "426", "240", "25", "https://example.com/video_240p/", segmentURLs),
							CreateRepresentation("720p", "1500000", "1280", "720", "30", "https://example.com/video_720p/", segmentURLs),
							CreateRepresentation("1080p", "3000000", "1920", "1080", "30", "https://example.com/video_1080p/", segmentURLs),
							CreateRepresentation("1440p", "6000000", "2560", "1440", "30", "https://example.com/video_1440p/", segmentURLs),
							CreateRepresentation("2160p", "12000000", "3840", "2160", "30", "https://example.com/video_2160p/", segmentURLs),
						},
					},
				},
			},
		},
	}

	file, err := os.Create(filepath.Join(outputDir, "output.mpd"))
	if err != nil {
		return fmt.Errorf("error creating MPD file: %w", err)
	}
	defer file.Close()

	encoder := xml.NewEncoder(file)
	encoder.Indent("  ", "    ")
	return encoder.Encode(mpd)
}

// createRepresentation creates a Representation for the MPD file
func CreateRepresentation(resolution, bandwidth, width, height, frameRate, baseURL string, segmentURLs []SegmentURL) Representation {
	return Representation{
		ID:        resolution,
		Bandwidth: bandwidth,
		Codecs:    "vp09.00.10.08",
		Width:     width,
		Height:    height,
		FrameRate: frameRate,
		BaseURL:   baseURL,
		SegmentList: SegmentList{
			Duration:    "2000000",
			Timescale:   "1000000",
			SegmentURLs: segmentURLs,
		},
	}
}
