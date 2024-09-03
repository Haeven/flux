package mpd

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// MPD represents the root element of the MPD file
type MPD struct {
	XMLName                   xml.Name `xml:"MPD"`
	XMLNs                     string   `xml:"xmlns,attr"`
	XMLNsXsi                  string   `xml:"xmlns:xsi,attr"`
	XsiSchemaLocation         string   `xml:"xsi:schemaLocation,attr"`
	MediaPresentationDuration string   `xml:"mediaPresentationDuration,attr"`
	MinBufferTime             string   `xml:"minBufferTime,attr"`
	Periods                   []Period `xml:"Period"`
}

// Period represents a period in the MPD file
type Period struct {
	XMLName        xml.Name        `xml:"Period"`
	Duration       string          `xml:"duration,attr"`
	AdaptationSets []AdaptationSet `xml:"AdaptationSet"`
}

// AdaptationSet represents an adaptation set in the MPD file
type AdaptationSet struct {
	XMLName         xml.Name         `xml:"AdaptationSet"`
	MimeType        string           `xml:"mimeType,attr"`
	Codecs          string           `xml:"codecs,attr"`
	Width           string           `xml:"width,attr"`
	Height          string           `xml:"height,attr"`
	FrameRate       string           `xml:"frameRate,attr"`
	Representations []Representation `xml:"Representation"`
}

// Representation represents a representation in the MPD file
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

// SegmentList represents the segment list in the MPD file
type SegmentList struct {
	XMLName     xml.Name     `xml:"SegmentList"`
	Duration    string       `xml:"duration,attr"`
	Timescale   string       `xml:"timescale,attr"`
	SegmentURLs []SegmentURL `xml:"SegmentURL"`
}

// SegmentURL represents the URL of a segment in the MPD file
type SegmentURL struct {
	XMLName xml.Name `xml:"SegmentURL"`
	Media   string   `xml:"media,attr"`
}

// GenerateMPD generates an MPD file for the video segments
func GenerateMPD(outputDir string) error {
	segmentFiles, err := filepath.Glob(filepath.Join(outputDir, "*_segment_*.webm"))
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
		MediaPresentationDuration: "PT" + strconv.Itoa(len(segmentURLs)*5) + "S", // Assuming 5-second segments
		MinBufferTime:             "PT1.5S",
		Periods: []Period{
			{
				Duration: "PT" + strconv.Itoa(len(segmentURLs)*5) + "S", // Assuming 5-second segments
				AdaptationSets: []AdaptationSet{
					{
						MimeType: "video/webm",
						Codecs:   "vp09.00.10.08",
						Representations: []Representation{
							createRepresentation("144p", "150000", "256", "144", "25", segmentURLs),
							createRepresentation("240p", "300000", "426", "240", "25", segmentURLs),
							createRepresentation("720p", "1500000", "1280", "720", "30", segmentURLs),
							createRepresentation("1080p", "3000000", "1920", "1080", "30", segmentURLs),
							createRepresentation("1440p", "6000000", "2560", "1440", "30", segmentURLs),
							createRepresentation("2160p", "12000000", "3840", "2160", "30", segmentURLs),
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
	if err := encoder.Encode(mpd); err != nil {
		return fmt.Errorf("error encoding MPD: %w", err)
	}

	return nil
}

// createRepresentation creates a Representation for the MPD file
func createRepresentation(id, bandwidth, width, height, frameRate string, segmentURLs []SegmentURL) Representation {
	return Representation{
		ID:        id,
		Bandwidth: bandwidth,
		Codecs:    "vp09.00.10.08",
		Width:     width,
		Height:    height,
		FrameRate: frameRate,
		SegmentList: SegmentList{
			Duration:    "5000000", // 5 seconds in microseconds
			Timescale:   "1000000", // 1 second in microseconds
			SegmentURLs: segmentURLs,
		},
	}
}
