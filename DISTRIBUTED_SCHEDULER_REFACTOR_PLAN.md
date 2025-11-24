# 分布式查询调度器重构计划

## 1. 现状分析

当前 `/workspace/data-processing-platform/src/query_engine/distributed_scheduler.rs` 文件包含以下功能：

- **查询计划结构**: `DistributedQueryPlan`, `SubQuery`
- **执行策略**: `Parallel`, `Sequential`, `Pipeline`
- **节点管理**: `ClusterNode` 结构和管理
- **任务调度**: 基础的查询提交和执行逻辑
- **故障恢复**: 基础的熔断器机制

## 2. 重构目标

### 2.1 完善查询分解功能
- 实现真正的查询计划分析和分解
- 支持复杂查询的智能分片
- 基于数据分布的优化分解

### 2.2 集成 DataFusion Ballista
- 使用 Ballista 作为底层分布式执行引擎
- 利用其任务调度和资源管理能力
- 实现容错和故障恢复机制

### 2.3 优化并行执行
- 真正的跨节点并行查询处理
- 智能节点选择和负载均衡
- 任务依赖管理和执行顺序控制

### 2.4 完善故障恢复
- 节点故障检测和处理
- 任务重调度机制
- 查询状态管理和恢复

## 3. 重构步骤

### 步骤 1: 创建新的模块结构

```
src/
└── query_engine/
    ├── distributed/
    │   ├── mod.rs
    │   ├── ballista_integration.rs
    │   ├── query_analyzer.rs
    │   ├── task_scheduler.rs
    │   ├── result_aggregator.rs
    │   └── fault_recovery.rs
    └── distributed_scheduler.rs  # 重构后的主调度器
```

### 步骤 2: 更新 Cargo.toml

```toml
[dependencies]
# 现有依赖保持不变
datafusion = { version = "40.0", features = ["parquet", "json", "crypto_expressions", "regex_expressions"] }
arrow = { version = "52.2", features = ["prettyprint"] }
# 添加 Ballista 依赖
ballista = "0.18"
ballista-core = "0.18"
ballista-executor = "0.18"
ballista-scheduler = "0.18"
```

### 步骤 3: 实现 Ballista 集成模块

**File: `src/query_engine/distributed/ballista_integration.rs`**

```rust
use std::sync::Arc;
use ballista::context::BallistaContext;
use ballista::prelude::{BallistaConfig, BALLISTA_DEFAULT_SCHEDULER_HOST, BALLISTA_DEFAULT_SCHEDULER_PORT};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::LogicalPlan;
use anyhow::Result;

pub struct BallistaExecutor {
    context: Arc<BallistaContext>,
}

impl BallistaExecutor {
    pub async fn new(scheduler_host: &str, scheduler_port: u16) -> Result<Self> {
        let config = BallistaConfig::builder()
            .set("ballista.shuffle.staging_dir", "/tmp")
            .set("ballista.concurrent.tasks", "4")
            .build()?;

        let context = BallistaContext::remote(scheduler_host, scheduler_port, &config).await?;
        
        Ok(Self {
            context: Arc::new(context),
        })
    }

    pub async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.context.sql(sql).await?;
        let results = df.collect().await?;
        Ok(results)
    }

    pub async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let plan = self.context.logical_plan(sql).await?;
        Ok(plan)
    }
}
```

### 步骤 4: 实现查询分析器

**File: `src/query_engine/distributed/query_analyzer.rs`**

```rust
use datafusion::logical_plan::LogicalPlan;
use crate::query_engine::distributed_scheduler::{DistributedQueryPlan, SubQuery, ExecutionStrategy};
use anyhow::Result;

pub struct QueryAnalyzer;

impl QueryAnalyzer {
    pub fn analyze(&self, logical_plan: &LogicalPlan, query_id: &str) -> Result<DistributedQueryPlan> {
        let subqueries = self.decompose_plan(logical_plan, query_id)?;
        let strategy = self.determine_strategy(logical_plan)?;
        
        Ok(DistributedQueryPlan {
            query_id: query_id.to_string(),
            original_query: format!("{:?}", logical_plan), // 实际应该从原始SQL恢复
            subqueries,
            execution_strategy: strategy,
        })
    }

    fn decompose_plan(&self, plan: &LogicalPlan, query_id: &str) -> Result<Vec<SubQuery>> {
        // 根据逻辑计划的结构分解为可并行执行的子任务
        let mut subqueries = Vec::new();
        
        match plan {
            // 对于表扫描操作，可以按分区进行分解
            datafusion::logical_plan::LogicalPlan::TableScan(table_scan) => {
                // 获取表的分区信息
                let partitions = self.get_table_partitions(&table_scan.table_name)?;
                
                for (idx, partition) in partitions.iter().enumerate() {
                    subqueries.push(SubQuery {
                        id: format!("{}_{}", query_id, idx),
                        sql: format!("SELECT * FROM {} WHERE partition_col = '{}'", 
                                   table_scan.table_name, partition),
                        target_node: None,
                        dependencies: Vec::new(),
                    });
                }
            }
            // 对于聚合操作，可能需要 Map-Reduce 模式
            datafusion::logical_plan::LogicalPlan::Aggregate(_) => {
                // 分解为 Map 阶段和 Reduce 阶段
                // Map 阶段在各个节点执行部分聚合
                // Reduce 阶段合并结果
                subqueries.push(SubQuery {
                    id: format!("{}_map", query_id),
                    sql: format!("PARTIAL_AGGREGATE_QUERY"), // 实际需要构建部分聚合查询
                    target_node: None,
                    dependencies: Vec::new(),
                });
            }
            // 其他操作的分解逻辑...
            _ => {
                // 默认情况下，将整个查询作为一个子查询
                subqueries.push(SubQuery {
                    id: format!("{}_0", query_id),
                    sql: format!("{:?}", plan), // 需要转换为实际SQL
                    target_node: None,
                    dependencies: Vec::new(),
                });
            }
        }
        
        Ok(subqueries)
    }

    fn determine_strategy(&self, plan: &LogicalPlan) -> Result<ExecutionStrategy> {
        // 根据查询计划的特性确定执行策略
        match plan {
            datafusion::logical_plan::LogicalPlan::Join(_) => {
                // 连接操作可能需要特殊的执行策略
                Ok(ExecutionStrategy::Pipeline)
            }
            datafusion::logical_plan::LogicalPlan::TableScan(_) => {
                // 表扫描通常可以并行执行
                Ok(ExecutionStrategy::Parallel)
            }
            _ => {
                // 默认使用并行策略
                Ok(ExecutionStrategy::Parallel)
            }
        }
    }

    fn get_table_partitions(&self, table_name: &str) -> Result<Vec<String>> {
        // 获取表的分区信息
        // 这里需要根据实际的数据源实现
        Ok(vec![]) // 简化实现
    }
}
```

### 步骤 5: 实现任务调度器

**File: `src/query_engine/distributed/task_scheduler.rs`**

```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::query_engine::distributed_scheduler::{SubQuery, ClusterNode, NodeStatus, QueryStatus};
use crate::query_engine::distributed::ballista_integration::BallistaExecutor;
use anyhow::Result;

pub struct TaskScheduler {
    ballista_executor: Arc<BallistaExecutor>,
    cluster_nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
    node_selector: Box<dyn NodeSelector>,
}

impl TaskScheduler {
    pub fn new(
        ballista_executor: Arc<BallistaExecutor>,
        node_selector: Box<dyn NodeSelector>
    ) -> Self {
        Self {
            ballista_executor,
            cluster_nodes: Arc::new(RwLock::new(HashMap::new())),
            node_selector,
        }
    }

    pub async fn schedule_subquery(&self, subquery: &SubQuery) -> Result<()> {
        let node_id = self.select_node_for_query(subquery).await?;
        
        // 通过 Ballista 执行子查询
        self.ballista_executor
            .execute_query(&subquery.sql)
            .await?;
        
        Ok(())
    }

    async fn select_node_for_query(&self, subquery: &SubQuery) -> Result<String> {
        match &subquery.target_node {
            Some(node_id) => {
                let nodes = self.cluster_nodes.read().await;
                if nodes.contains_key(node_id) {
                    Ok(node_id.clone())
                } else {
                    Err(anyhow::anyhow!("Target node not found: {}", node_id))
                }
            }
            None => {
                let nodes = self.cluster_nodes.read().await;
                let available_nodes: Vec<_> = nodes.values()
                    .filter(|node| node.status == NodeStatus::Active)
                    .cloned()
                    .collect();
                
                if available_nodes.is_empty() {
                    return Err(anyhow::anyhow!("No active nodes available"));
                }
                
                let selected_node = self.node_selector.select_node(&available_nodes)?;
                Ok(selected_node.id)
            }
        }
    }

    pub async fn add_cluster_node(&self, node: ClusterNode) {
        let mut nodes = self.cluster_nodes.write().await;
        nodes.insert(node.id.clone(), node);
    }
}

pub trait NodeSelector: Send + Sync {
    fn select_node(&self, nodes: &[ClusterNode]) -> Result<ClusterNode>;
}

pub struct LoadBasedNodeSelector;

impl NodeSelector for LoadBasedNodeSelector {
    fn select_node(&self, nodes: &[ClusterNode]) -> Result<ClusterNode> {
        if nodes.is_empty() {
            return Err(anyhow::anyhow!("No nodes available"));
        }

        // 选择资源使用率最低的节点
        let selected = nodes
            .iter()
            .filter(|node| node.status == NodeStatus::Active)
            .min_by(|a, b| {
                let load_a = (a.cpu_usage + a.memory_usage as f64) / 2.0 + a.active_queries as f64 * 0.1;
                let load_b = (b.cpu_usage + b.memory_usage as f64) / 2.0 + b.active_queries as f64 * 0.1;
                load_a.partial_cmp(&load_b).unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| anyhow::anyhow!("No active nodes available"))?;

        Ok(selected.clone())
    }
}
```

### 步骤 6: 实现结果聚合器

**File: `src/query_engine/distributed/result_aggregator.rs`**

```rust
use std::collections::HashMap;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::error::ArrowError;
use anyhow::Result;

pub struct ResultAggregator;

impl ResultAggregator {
    pub async fn aggregate(&self, subquery_results: &HashMap<String, Vec<RecordBatch>>) -> Result<Vec<RecordBatch>> {
        // 根据查询类型和子查询之间的关系进行结果聚合
        let mut all_batches = Vec::new();
        
        for batches in subquery_results.values() {
            all_batches.extend(batches.clone());
        }
        
        // 如果需要特定的聚合逻辑，可以在这里实现
        // 例如：合并相同 schema 的 RecordBatch，处理 JOIN 结果等
        self.merge_batches(all_batches).await
    }

    async fn merge_batches(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        // 简单的合并实现，实际可能需要更复杂的逻辑
        Ok(batches)
    }

    pub async fn aggregate_join_results(&self, left: Vec<RecordBatch>, right: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        // 实现连接结果的聚合逻辑
        // 这需要根据具体的连接类型和条件实现
        let mut result = left;
        result.extend(right);
        Ok(result)
    }
}
```

### 步骤 7: 实现故障恢复机制

**File: `src/query_engine/distributed/fault_recovery.rs`**

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::query_engine::distributed_scheduler::{SubQuery, ClusterNode, NodeStatus};
use crate::query_engine::distributed::ballista_integration::BallistaExecutor;
use anyhow::Result;

pub struct FaultRecoveryManager {
    cluster_nodes: Arc<RwLock<HashMap<String, NodeHealth>>>,
    max_retry_attempts: u32,
}

#[derive(Debug, Clone)]
pub struct NodeHealth {
    pub node: ClusterNode,
    pub failure_count: u32,
    pub last_failure_time: Option<std::time::Instant>,
    pub status: NodeHealthStatus,
}

#[derive(Debug, Clone)]
pub enum NodeHealthStatus {
    Healthy,
    Unhealthy,
    Recovering,
}

impl FaultRecoveryManager {
    pub fn new(max_retry_attempts: u32) -> Self {
        Self {
            cluster_nodes: Arc::new(RwLock::new(HashMap::new())),
            max_retry_attempts,
        }
    }

    pub async fn record_node_failure(&self, node_id: &str) {
        let mut nodes = self.cluster_nodes.write().await;
        let health = nodes.entry(node_id.to_string()).or_insert_with(|| NodeHealth {
            node: ClusterNode {
                id: node_id.to_string(),
                address: String::new(),
                cpu_usage: 0.0,
                memory_usage: 0.0,
                active_queries: 0,
                max_concurrent_queries: 10,
                status: NodeStatus::Active,
            },
            failure_count: 0,
            last_failure_time: None,
            status: NodeHealthStatus::Healthy,
        });

        health.failure_count += 1;
        health.last_failure_time = Some(std::time::Instant::now());
        
        if health.failure_count >= self.max_retry_attempts {
            health.status = NodeHealthStatus::Unhealthy;
            // 标记节点为不健康，不再分配任务
        }
    }

    pub async fn record_node_success(&self, node_id: &str) {
        let mut nodes = self.cluster_nodes.write().await;
        if let Some(health) = nodes.get_mut(node_id) {
            health.failure_count = 0;
            health.status = NodeHealthStatus::Healthy;
        }
    }

    pub async fn is_node_healthy(&self, node_id: &str) -> bool {
        let nodes = self.cluster_nodes.read().await;
        match nodes.get(node_id) {
            Some(health) => matches!(health.status, NodeHealthStatus::Healthy),
            None => true, // 假设未记录的节点是健康的
        }
    }

    pub async fn reschedule_failed_query(&self, original_subquery: &SubQuery, failed_node_id: &str) -> Result<SubQuery> {
        // 标记失败的节点
        self.record_node_failure(failed_node_id).await;
        
        // 创建新的子查询，排除失败的节点
        let mut new_subquery = original_subquery.clone();
        
        // 在实现中，这里会寻找一个健康的节点来重新执行查询
        // 可能需要更新查询的执行计划以适应新的节点
        
        Ok(new_subquery)
    }

    pub async fn get_healthy_nodes(&self) -> Vec<String> {
        let nodes = self.cluster_nodes.read().await;
        nodes.iter()
            .filter(|(_, health)| matches!(health.status, NodeHealthStatus::Healthy))
            .map(|(id, _)| id.clone())
            .collect()
    }
}
```

### 步骤 8: 重构主调度器

**File: `src/query_engine/distributed_scheduler.rs` (重构后)**

```rust
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use uuid::Uuid;

use crate::query_engine::distributed::{
    ballista_integration::BallistaExecutor,
    query_analyzer::QueryAnalyzer,
    task_scheduler::{TaskScheduler, LoadBasedNodeSelector},
    result_aggregator::ResultAggregator,
    fault_recovery::FaultRecoveryManager,
};
use crate::query_engine::QueryEngine;

// 保持现有的数据结构定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedQueryPlan {
    pub query_id: String,
    pub original_query: String,
    pub subqueries: Vec<SubQuery>,
    pub execution_strategy: ExecutionStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubQuery {
    pub id: String,
    pub sql: String,
    pub target_node: Option<String>,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStrategy {
    Parallel,
    Sequential,
    Pipeline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecutionResult {
    pub query_id: String,
    pub subquery_results: HashMap<String, Vec<RecordBatch>>,
    pub final_result: Option<Vec<RecordBatch>>,
    pub execution_time_ms: u128,
    pub status: QueryStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub id: String,
    pub address: String,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub active_queries: usize,
    pub max_concurrent_queries: usize,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeStatus {
    Active,
    Inactive,
    Unhealthy,
}

pub struct DistributedQueryScheduler {
    ballista_executor: Arc<BallistaExecutor>,
    query_analyzer: Arc<QueryAnalyzer>,
    task_scheduler: Arc<TaskScheduler>,
    result_aggregator: Arc<ResultAggregator>,
    fault_recovery: Arc<FaultRecoveryManager>,
    
    // 现有的状态管理
    cluster_nodes: Arc<RwLock<HashMap<String, ClusterNode>>>,
    query_queue: Arc<RwLock<Vec<String>>>,
    active_queries: Arc<RwLock<HashMap<String, DistributedQueryPlan>>>,
    query_results: Arc<RwLock<HashMap<String, QueryExecutionResult>>>,
}

impl DistributedQueryScheduler {
    pub async fn new(scheduler_host: &str, scheduler_port: u16) -> Result<Self> {
        let ballista_executor = Arc::new(
            BallistaExecutor::new(scheduler_host, scheduler_port).await?
        );
        
        let query_analyzer = Arc::new(QueryAnalyzer);
        
        let task_scheduler = Arc::new(TaskScheduler::new(
            ballista_executor.clone(),
            Box::new(LoadBasedNodeSelector)
        ));
        
        let result_aggregator = Arc::new(ResultAggregator);
        
        let fault_recovery = Arc::new(FaultRecoveryManager::new(3)); // 最大重试3次

        Ok(Self {
            ballista_executor,
            query_analyzer,
            task_scheduler,
            result_aggregator,
            fault_recovery,
            cluster_nodes: Arc::new(RwLock::new(HashMap::new())),
            query_queue: Arc::new(RwLock::new(Vec::new())),
            active_queries: Arc::new(RwLock::new(HashMap::new())),
            query_results: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn submit_query(&self, query: &str) -> Result<String> {
        let query_id = Uuid::new_v4().to_string();
        
        // 使用 Ballista 分析查询并生成执行计划
        let logical_plan = self.ballista_executor.create_logical_plan(query).await?;
        let plan = self.query_analyzer.analyze(&logical_plan, &query_id)?;
        
        // 加入查询队列
        {
            let mut queue = self.query_queue.write().await;
            queue.push(query_id.clone());
        }
        
        // 存储计划
        {
            let mut active_queries = self.active_queries.write().await;
            active_queries.insert(query_id.clone(), plan);
        }

        Ok(query_id)
    }

    pub async fn execute_query(&self, query_id: &str) -> Result<QueryExecutionResult> {
        use std::time::Instant;
        
        let start_time = Instant::now();
        
        // 获取查询计划
        let plan = {
            let active_queries = self.active_queries.read().await;
            active_queries.get(query_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("Query plan not found: {}", query_id))?
        };

        // 根据执行策略执行子查询
        let subquery_results = match &plan.execution_strategy {
            ExecutionStrategy::Parallel => self.execute_parallel(&plan).await?,
            ExecutionStrategy::Sequential => self.execute_sequential(&plan).await?,
            ExecutionStrategy::Pipeline => self.execute_pipeline(&plan).await?,
        };

        // 聚合结果
        let final_result = Some(
            self.result_aggregator.aggregate(&subquery_results).await?
        );

        let execution_time = start_time.elapsed().as_millis();

        let result = QueryExecutionResult {
            query_id: query_id.to_string(),
            subquery_results,
            final_result,
            execution_time_ms: execution_time,
            status: QueryStatus::Completed,
        };

        // 存储结果
        {
            let mut results = self.query_results.write().await;
            results.insert(query_id.to_string(), result.clone());
        }

        // 从活跃查询中移除
        {
            let mut active_queries = self.active_queries.write().await;
            active_queries.remove(query_id);
        }

        Ok(result)
    }

    async fn execute_parallel(&self, plan: &DistributedQueryPlan) -> Result<HashMap<String, Vec<RecordBatch>>> {
        let mut handles = Vec::new();
        let mut results = HashMap::new();

        for subquery in &plan.subqueries {
            let task_scheduler = self.task_scheduler.clone();
            let subquery_clone = subquery.clone();
            
            let handle = tokio::spawn(async move {
                task_scheduler.schedule_subquery(&subquery_clone).await
            });
            
            handles.push((subquery.id.clone(), handle));
        }

        // 收集结果
        for (id, handle) in handles {
            match handle.await {
                Ok(Ok(())) => {
                    // 这里需要从 Ballista 获取实际的执行结果
                    // 简化示例，实际需要实现结果获取逻辑
                    results.insert(id, Vec::new());
                }
                Ok(Err(e)) => {
                    return Err(anyhow::anyhow!("Subquery execution failed: {}", e));
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Subquery task failed: {}", e));
                }
            }
        }

        Ok(results)
    }

    async fn execute_sequential(&self, plan: &DistributedQueryPlan) -> Result<HashMap<String, Vec<RecordBatch>>> {
        let mut results = HashMap::new();

        for subquery in &plan.subqueries {
            self.task_scheduler.schedule_subquery(subquery).await?;
            results.insert(subquery.id.clone(), Vec::new());
        }

        Ok(results)
    }

    async fn execute_pipeline(&self, _plan: &DistributedQueryPlan) -> Result<HashMap<String, Vec<RecordBatch>>> {
        // 流水线执行逻辑
        self.execute_sequential(_plan).await
    }

    pub async fn add_cluster_node(&self, node: ClusterNode) {
        self.task_scheduler.add_cluster_node(node).await;
        let mut nodes = self.cluster_nodes.write().await;
        nodes.insert(node.id.clone(), node);
    }
}
```

## 4. 集成测试计划

### 4.1 单元测试
- 查询分析器的分解逻辑测试
- 任务调度器的节点选择测试
- 结果聚合器的合并逻辑测试
- 故障恢复机制的故障模拟测试

### 4.2 集成测试
- 端到端的分布式查询执行测试
- 多节点并行执行性能测试
- 节点故障和恢复测试
- 复杂查询分解和执行测试

## 5. 性能优化建议

### 5.1 查询优化
- 查询计划的优化和重写
- 统计信息的收集和使用
- 连接顺序的优化

### 5.2 资源管理
- 内存使用优化
- 网络传输优化
- 磁盘 I/O 优化

### 5.3 负载均衡
- 动态负载均衡策略
- 数据本地性优化
- 执行资源的动态分配

## 6. 监控和运维

### 6.1 指标收集
- 查询执行时间
- 资源使用情况
- 节点健康状态
- 故障恢复统计

### 6.2 日志记录
- 查询执行日志
- 节点状态变更日志
- 故障和恢复事件日志

通过以上重构计划，可以将现有的基础分布式调度器升级为一个功能完整的、基于 DataFusion Ballista 的分布式查询执行系统，具备智能查询分解、跨节点并行执行、结果聚合和故障恢复等完整功能。