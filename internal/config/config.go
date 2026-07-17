// Package config loads the cluster topology from YAML.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Node struct {
	ID   uint32 `yaml:"id"`
	Addr string `yaml:"addr"`
}

type Cluster struct {
	ID        uint32 `yaml:"id"`
	ShardFrom uint64 `yaml:"shard_from"`
	ShardTo   uint64 `yaml:"shard_to"`
	Nodes     []Node `yaml:"nodes"`
}

type Config struct {
	InitialBalance int64     `yaml:"initial_balance"`
	Clusters       []Cluster `yaml:"clusters"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &c, nil
}

// FindNode returns the node and its cluster for a given node ID.
func (c *Config) FindNode(id uint32) (*Node, *Cluster, error) {
	for i := range c.Clusters {
		cl := &c.Clusters[i]
		for j := range cl.Nodes {
			if cl.Nodes[j].ID == id {
				return &cl.Nodes[j], cl, nil
			}
		}
	}
	return nil, nil, fmt.Errorf("node %d not in config", id)
}

// ClusterForItem returns the cluster whose shard range contains itemID.
// (It'll be used for cross-shard routing.)
func (c *Config) ClusterForItem(itemID uint64) (*Cluster, error) {
	for i := range c.Clusters {
		cl := &c.Clusters[i]
		if itemID >= cl.ShardFrom && itemID <= cl.ShardTo {
			return cl, nil
		}
	}
	return nil, fmt.Errorf("item %d not in any shard", itemID)
}
