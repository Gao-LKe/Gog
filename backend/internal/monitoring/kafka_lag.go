package monitoring

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaLagSampleTimeout          = 5 * time.Second
	kafkaLagMaxPartitions          = 128
	kafkaLagMaxRecordsPerPartition = 256
)

// KafkaLagSnapshot describes an observation of a consumer group's uncommitted
// offsets. Overdue is -1 because Kafka offsets cannot identify how many distinct
// order requests are still awaiting a terminal database result.
type KafkaLagSnapshot struct {
	ObservedAt time.Time
	State      string
	Lag        int64
	Overdue    int64
	OldestAge  time.Duration
	Partitions []KafkaPartitionLag
}

type KafkaPartitionLag struct {
	Partition int
	Lag       int64
	OldestAge time.Duration
}

// SampleKafkaLag reads Kafka without joining the group or modifying offsets.
// The whole observation is limited to five seconds; missing topic or committed
// group positions are reported as not_integrated instead of a healthy zero.
func SampleKafkaLag(ctx context.Context, brokers []string, topic, group string, overdueAfter time.Duration) (KafkaLagSnapshot, error) {
	snapshot := KafkaLagSnapshot{ObservedAt: time.Now(), State: "unavailable", Overdue: -1}
	if topic == "" || group == "" {
		snapshot.State = "not_integrated"
		return snapshot, nil
	}
	if len(brokers) == 0 {
		return snapshot, errors.New("kafka lag: no brokers configured")
	}
	// Overdue requires durable request state; the threshold is intentionally not
	// applied to offsets, whose timestamps may be out of order or absent.
	_ = overdueAfter

	ctx, cancel := context.WithTimeout(ctx, kafkaLagSampleTimeout)
	defer cancel()
	client := kafka.Client{Addr: kafka.TCP(brokers...), Timeout: kafkaLagSampleTimeout}
	metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return snapshot, fmt.Errorf("kafka lag metadata: %w", err)
	}
	var partitions []int
	for _, t := range metadata.Topics {
		if t.Name != topic {
			continue
		}
		if errors.Is(t.Error, kafka.UnknownTopicOrPartition) {
			snapshot.State = "not_integrated"
			return snapshot, nil
		}
		if t.Error != nil {
			return snapshot, fmt.Errorf("kafka lag topic metadata: %w", t.Error)
		}
		for _, p := range t.Partitions {
			if p.Error != nil {
				return snapshot, fmt.Errorf("kafka lag partition %d metadata: %w", p.ID, p.Error)
			}
			partitions = append(partitions, p.ID)
		}
	}
	if len(partitions) == 0 {
		snapshot.State = "not_integrated"
		return snapshot, nil
	}
	if len(partitions) > kafkaLagMaxPartitions {
		return snapshot, fmt.Errorf("kafka lag: %d partitions exceed sample limit %d", len(partitions), kafkaLagMaxPartitions)
	}
	sort.Ints(partitions)

	committedResponse, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: group, Topics: map[string][]int{topic: partitions},
	})
	if err != nil {
		return snapshot, fmt.Errorf("kafka lag committed offsets: %w", err)
	}
	if errors.Is(committedResponse.Error, kafka.GroupIdNotFound) {
		snapshot.State = "not_integrated"
		return snapshot, nil
	}
	if committedResponse.Error != nil {
		return snapshot, fmt.Errorf("kafka lag consumer group: %w", committedResponse.Error)
	}
	committed := make(map[int]int64, len(partitions))
	for _, p := range committedResponse.Topics[topic] {
		if p.Error != nil {
			return snapshot, fmt.Errorf("kafka lag partition %d committed offset: %w", p.Partition, p.Error)
		}
		committed[p.Partition] = p.CommittedOffset
	}
	if len(committed) != len(partitions) {
		return snapshot, errors.New("kafka lag: incomplete committed offsets")
	}
	allUncommitted := true
	for _, partition := range partitions {
		if committed[partition] >= 0 {
			allUncommitted = false
			break
		}
	}
	if allUncommitted {
		snapshot.State = "not_integrated"
		return snapshot, nil
	}

	requests := make([]kafka.OffsetRequest, 0, len(partitions)*2)
	for _, partition := range partitions {
		requests = append(requests, kafka.FirstOffsetOf(partition), kafka.LastOffsetOf(partition))
	}
	listed, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: requests}})
	if err != nil {
		return snapshot, fmt.Errorf("kafka lag log offsets: %w", err)
	}
	ends := make(map[int]kafka.PartitionOffsets, len(partitions))
	for _, p := range listed.Topics[topic] {
		if p.Error != nil {
			return snapshot, fmt.Errorf("kafka lag partition %d log offsets: %w", p.Partition, p.Error)
		}
		ends[p.Partition] = p
	}
	if len(ends) != len(partitions) {
		return snapshot, errors.New("kafka lag: incomplete log offsets")
	}
	for _, partition := range partitions {
		start := committed[partition]
		end := ends[partition]
		lag, err := partitionLag(start, end.FirstOffset, end.LastOffset)
		if err != nil {
			return snapshot, fmt.Errorf("kafka lag partition %d: %w", partition, err)
		}
		part := KafkaPartitionLag{Partition: partition, Lag: lag}
		if lag > 0 {
			fetched, err := client.Fetch(ctx, &kafka.FetchRequest{
				Topic: topic, Partition: partition, Offset: start,
				MinBytes: 1, MaxBytes: 64 << 10, MaxWait: 250 * time.Millisecond,
			})
			if err != nil {
				return snapshot, fmt.Errorf("kafka lag partition %d oldest record: %w", partition, err)
			}
			if fetched.Error != nil {
				return snapshot, fmt.Errorf("kafka lag partition %d oldest record: %w", partition, fetched.Error)
			}
			part.OldestAge, err = firstRecordAge(fetched.Records, start, snapshot.ObservedAt)
			if err != nil {
				return snapshot, fmt.Errorf("kafka lag partition %d oldest record: %w", partition, err)
			}
		}
		snapshot.Lag += lag
		if part.OldestAge > snapshot.OldestAge {
			snapshot.OldestAge = part.OldestAge
		}
		snapshot.Partitions = append(snapshot.Partitions, part)
	}
	snapshot.State = "ok"
	return snapshot, nil
}

func partitionLag(committed, first, last int64) (int64, error) {
	if committed < 0 {
		return 0, errors.New("partition has no committed offset")
	}
	if first < 0 || last < first {
		return 0, errors.New("invalid log offset range")
	}
	if committed < first {
		return 0, errors.New("committed offset expired from log")
	}
	if committed > last {
		return 0, errors.New("committed offset exceeds log end")
	}
	return last - committed, nil
}

func firstRecordAge(records kafka.RecordReader, committed int64, observedAt time.Time) (time.Duration, error) {
	for i := 0; i < kafkaLagMaxRecordsPerPartition; i++ {
		record, err := records.ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, errors.New("no unprocessed record in bounded fetch")
			}
			return 0, err
		}
		offset, recordedAt := record.Offset, record.Time
		if record.Key != nil {
			_ = record.Key.Close()
		}
		if record.Value != nil {
			_ = record.Value.Close()
		}
		if offset < committed {
			continue
		}
		if recordedAt.IsZero() {
			return 0, errors.New("oldest record has no timestamp")
		}
		age := observedAt.Sub(recordedAt)
		if age < 0 {
			return 0, nil
		}
		return age, nil
	}
	return 0, errors.New("oldest record not found within bounded scan")
}
