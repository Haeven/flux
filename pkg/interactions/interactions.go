// pkg/interactions/interactions.go
package interactions

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"flux/pkg/db"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/uptrace/bun"
)

type InteractionService struct {
	db     *bun.DB
	socket *socketio.Server // Add this line
}

type Interaction struct {
	bun.BaseModel `bun:"table:interactions"`

	ID        int64     `bun:"id,pk,autoincrement"`
	VideoID   string    `bun:"video_id,notnull"`
	UserID    string    `bun:"user_id,notnull"`
	Type      string    `bun:"type,notnull"`
	Content   string    `bun:"content"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

func NewInteractionService(socket *socketio.Server) *InteractionService {
	return &InteractionService{
		db:     db.Initialize(),
		socket: socket,
	}
}

func (s *InteractionService) SubscribeToEvents() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "interaction_group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{"likes", "dislikes", "comments"}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %v", err)
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v", err)
			continue
		}

		switch *msg.TopicPartition.Topic {
		case "likes":
			s.handleLike(msg)
		case "dislikes":
			s.handleDislike(msg)
		case "comments":
			s.handleComment(msg)
		}
	}
}

func (s *InteractionService) handleLike(msg *kafka.Message) {
	var data struct {
		VideoID string `json:"video_id"`
		UserID  string `json:"user_id"`
	}
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		log.Printf("Error unmarshaling like message: %v", err)
		return
	}

	interaction := &Interaction{
		VideoID:   data.VideoID,
		UserID:    data.UserID,
		Type:      "like",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err := s.db.NewInsert().Model(interaction).Exec(context.Background())
	if err != nil {
		log.Printf("Error inserting like: %v", err)
	} else {
		// Emit Socket.io event
		s.socket.BroadcastToRoom("", interaction.VideoID, "new_like", interaction)
	}
}

func (s *InteractionService) handleDislike(msg *kafka.Message) {
	var data struct {
		VideoID string `json:"video_id"`
		UserID  string `json:"user_id"`
	}
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		log.Printf("Error unmarshaling dislike message: %v", err)
		return
	}

	interaction := &Interaction{
		VideoID:   data.VideoID,
		UserID:    data.UserID,
		Type:      "dislike",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err := s.db.NewInsert().Model(interaction).Exec(context.Background())
	if err != nil {
		log.Printf("Error inserting dislike: %v", err)
	} else {
		// Emit Socket.io event
		s.socket.BroadcastToRoom("", interaction.VideoID, "new_dislike", interaction)
	}
}

func (s *InteractionService) handleComment(msg *kafka.Message) {
	var data struct {
		VideoID string `json:"video_id"`
		UserID  string `json:"user_id"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		log.Printf("Error unmarshaling comment message: %v", err)
		return
	}

	interaction := &Interaction{
		VideoID:   data.VideoID,
		UserID:    data.UserID,
		Type:      "comment",
		Content:   data.Content,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err := s.db.NewInsert().Model(interaction).Exec(context.Background())
	if err != nil {
		log.Printf("Error inserting comment: %v", err)
	} else {
		// Emit Socket.io event
		s.socket.BroadcastToRoom("", interaction.VideoID, "new_comment", interaction)
	}
}

func (s *InteractionService) GetInteractions(videoID string) ([]Interaction, error) {
	var interactions []Interaction
	err := s.db.NewSelect().
		Model(&interactions).
		Where("video_id = ?", videoID).
		Order("created_at DESC").
		Scan(context.Background())

	return interactions, err
}

// Additional helper functions for database operations can be added here
