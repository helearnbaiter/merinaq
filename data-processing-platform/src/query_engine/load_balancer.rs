//! Flight SQL Load Balancer
//! 
//! This module implements load balancing functionality for Flight SQL services
//! to support cluster deployment and distribute query load across multiple nodes.

use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{info, debug, error};
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct FlightNode {
    pub id: String,
    pub address: String,
    pub is_healthy: bool,
    pub current_load: u32,
    pub max_load: u32,
    pub last_heartbeat: std::time::SystemTime,
}

impl FlightNode {
    pub fn new(id: String, address: String) -> Self {
        Self {
            id,
            address,
            is_healthy: true,
            current_load: 0,
            max_load: 100, // Default max load
            last_heartbeat: std::time::SystemTime::now(),
        }
    }

    pub fn load_ratio(&self) -> f64 {
        if self.max_load == 0 {
            return 1.0; // If max_load is 0, consider it fully loaded
        }
        self.current_load as f64 / self.max_load as f64
    }
}

pub enum LoadBalancingStrategy {
    RoundRobin,
    LeastLoad,
    Random,
    Weighted,
}

pub struct FlightLoadBalancer {
    nodes: Arc<RwLock<HashMap<String, FlightNode>>>,
    strategy: LoadBalancingStrategy,
    current_index: Arc<RwLock<usize>>,
    health_check_interval: std::time::Duration,
}

impl FlightLoadBalancer {
    pub fn new(strategy: LoadBalancingStrategy) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            strategy,
            current_index: Arc::new(RwLock::new(0)),
            health_check_interval: std::time::Duration::from_secs(30), // 30 seconds
        }
    }

    pub async fn add_node(&self, node: FlightNode) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node.id.clone(), node);
        info!("Added Flight SQL node: {}", node.id);
    }

    pub async fn remove_node(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if nodes.remove(node_id).is_some() {
            info!("Removed Flight SQL node: {}", node_id);
        }
    }

    pub async fn update_node_load(&self, node_id: &str, load: u32) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.current_load = load;
        }
    }

    pub async fn mark_node_healthy(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.is_healthy = true;
            node.last_heartbeat = std::time::SystemTime::now();
        }
    }

    pub async fn mark_node_unhealthy(&self, node_id: &str) {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(node_id) {
            node.is_healthy = false;
        }
    }

    pub async fn get_available_nodes(&self) -> Vec<FlightNode> {
        let nodes = self.nodes.read().await;
        nodes.values()
            .filter(|node| node.is_healthy)
            .cloned()
            .collect()
    }

    pub async fn select_node(&self) -> Option<FlightNode> {
        let available_nodes = self.get_available_nodes().await;
        
        if available_nodes.is_empty() {
            return None;
        }

        match &self.strategy {
            LoadBalancingStrategy::RoundRobin => {
                self.select_node_round_robin(&available_nodes).await
            }
            LoadBalancingStrategy::LeastLoad => {
                self.select_node_least_load(&available_nodes).await
            }
            LoadBalancingStrategy::Random => {
                self.select_node_random(&available_nodes).await
            }
            LoadBalancingStrategy::Weighted => {
                self.select_node_weighted(&available_nodes).await
            }
        }
    }

    async fn select_node_round_robin(&self, nodes: &[FlightNode]) -> Option<FlightNode> {
        if nodes.is_empty() {
            return None;
        }

        let mut index = self.current_index.write().await;
        let node = nodes[*index % nodes.len()].clone();
        *index += 1;
        Some(node)
    }

    async fn select_node_least_load(&self, nodes: &[FlightNode]) -> Option<FlightNode> {
        nodes.iter()
            .filter(|node| node.is_healthy)
            .min_by(|a, b| a.current_load.cmp(&b.current_load))
            .cloned()
    }

    async fn select_node_random(&self, nodes: &[FlightNode]) -> Option<FlightNode> {
        if nodes.is_empty() {
            return None;
        }

        use rand::Rng;
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..nodes.len());
        Some(nodes[index].clone())
    }

    async fn select_node_weighted(&self, nodes: &[FlightNode]) -> Option<FlightNode> {
        if nodes.is_empty() {
            return None;
        }

        // Calculate total weight (inverse of load ratio for load balancing)
        let total_weight: f64 = nodes.iter()
            .filter(|node| node.is_healthy)
            .map(|node| {
                let load_ratio = node.load_ratio();
                if load_ratio >= 1.0 {
                    0.0 // Don't select fully loaded nodes
                } else {
                    (1.0 - load_ratio) * 100.0 // Higher weight for lower load
                }
            })
            .sum();

        if total_weight == 0.0 {
            // If all nodes are fully loaded, return the least loaded one
            return self.select_node_least_load(nodes).await;
        }

        // Select based on weight
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let random_weight = rng.gen_range(0.0..total_weight);

        let mut current_weight = 0.0;
        for node in nodes {
            if !node.is_healthy {
                continue;
            }
            let load_ratio = node.load_ratio();
            let weight = if load_ratio >= 1.0 { 0.0 } else { (1.0 - load_ratio) * 100.0 };
            current_weight += weight;
            
            if random_weight <= current_weight {
                return Some(node.clone());
            }
        }

        // Fallback to least load
        self.select_node_least_load(nodes).await
    }

    pub async fn start_health_check(&self) {
        let nodes_clone = self.nodes.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                
                let node_ids: Vec<String> = {
                    let nodes = nodes_clone.read().await;
                    nodes.keys().cloned().collect()
                };

                for node_id in node_ids {
                    let node_exists = {
                        let nodes = nodes_clone.read().await;
                        nodes.contains_key(&node_id)
                    };

                    if node_exists {
                        if let Err(e) = Self::check_node_health(&node_id).await {
                            error!("Health check failed for node {}: {}", node_id, e);
                            let mut nodes = nodes_clone.write().await;
                            if let Some(node) = nodes.get_mut(&node_id) {
                                node.is_healthy = false;
                            }
                        } else {
                            let mut nodes = nodes_clone.write().await;
                            if let Some(node) = nodes.get_mut(&node_id) {
                                node.is_healthy = true;
                                node.last_heartbeat = std::time::SystemTime::now();
                            }
                            debug!("Health check passed for node: {}", node_id);
                        }
                    }
                }
            }
        });
    }

    async fn check_node_health(node_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // In a real implementation, this would make an actual health check request to the node
        // For now, we'll simulate a health check
        debug!("Performing health check for node: {}", node_id);
        
        // Simulate a health check (in real implementation, connect to the node and check)
        Ok(())
    }

    pub async fn get_cluster_stats(&self) -> ClusterStats {
        let nodes = self.nodes.read().await;
        let total_nodes = nodes.len();
        let healthy_nodes: usize = nodes.values().filter(|n| n.is_healthy).count();
        let total_current_load: u32 = nodes.values().map(|n| n.current_load).sum();
        let total_max_load: u32 = nodes.values().map(|n| n.max_load).sum();

        ClusterStats {
            total_nodes,
            healthy_nodes,
            total_current_load,
            total_max_load,
            average_load_ratio: if total_max_load > 0 {
                total_current_load as f64 / total_max_load as f64
            } else {
                0.0
            },
        }
    }
}

#[derive(Debug)]
pub struct ClusterStats {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub total_current_load: u32,
    pub total_max_load: u32,
    pub average_load_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_load_balancer() {
        let lb = FlightLoadBalancer::new(LoadBalancingStrategy::RoundRobin);
        
        let node1 = FlightNode::new("node1".to_string(), "localhost:9090".to_string());
        let node2 = FlightNode::new("node2".to_string(), "localhost:9091".to_string());
        
        lb.add_node(node1).await;
        lb.add_node(node2).await;
        
        assert_eq!(lb.get_available_nodes().await.len(), 2);
        
        let selected_node = lb.select_node().await;
        assert!(selected_node.is_some());
    }
}